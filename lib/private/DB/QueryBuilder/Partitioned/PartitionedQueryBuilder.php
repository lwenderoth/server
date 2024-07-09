<?php

declare(strict_types=1);
/**
 * @copyright Copyright (c) 2024 Robin Appelman <robin@icewind.nl>
 *
 * @license GNU AGPL version 3 or any later version
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

namespace OC\DB\QueryBuilder\Partitioned;

use OC\DB\ConnectionAdapter;
use OC\DB\QueryBuilder\CompositeExpression;
use OC\DB\QueryBuilder\ExtendedQueryBuilder;
use OC\DB\QueryBuilder\Sharded\ShardConnectionManager;
use OC\DB\QueryBuilder\Sharded\ShardedQueryBuilder;
use OC\SystemConfig;
use OCP\DB\IResult;
use OCP\DB\QueryBuilder\IQueryBuilder;
use OCP\IDBConnection;
use Psr\Log\LoggerInterface;

class PartitionedQueryBuilder extends ExtendedQueryBuilder {
	/** @var array<string, PartitionQuery> $splitQueries */
	private array $splitQueries = [];
	/** @var list<PartitionSplit> */
	private array $partitions = [];

	/** @var array{'column': string, 'alias': ?string}[] */
	private array $selects = [];
	private ?PartitionSplit $mainPartition = null;
	private bool $hasPositionalParameter = false;

	public function __construct(
		private ConnectionAdapter              $connection,
		private SystemConfig                   $systemConfig,
		private LoggerInterface                $logger,
		private array                  $shardDefinitions,
		private ShardConnectionManager $shardConnectionManager,
	) {
		parent::__construct($this->newQuery());
	}

	private function newQuery(): IQueryBuilder {
		return new ShardedQueryBuilder(
			$this->connection,
			$this->systemConfig,
			$this->logger,
			$this->shardDefinitions,
			$this->shardConnectionManager,
		);
	}

	// we need to save selects until we know all the table aliases
	public function select(...$selects) {
		$this->selects = [];
		$this->addSelect(...$selects);
		return $this;
	}

	public function addSelect(...$selects) {
		$selects = array_map(function($select) {
			return ['select' => $select, 'alias' => null];
		}, $select);
		$this->selects = array_merge($this->selects, $select);
		return $this;
	}

	public function selectAlias($select, $alias) {
		$this->selects[] = ['select' => $select, 'alias' => $alias];
		return $this;
	}

	private function ensureSelect(string $column) {
		$checkColumn = $column;
		if (str_contains($checkColumn, '.')) {
			[, $checkColumn] = explode('.', $checkColumn);
		}
		foreach ($this->selects as $select) {
			if ($select['select'] === $checkColumn || $select['select'] === '*' || str_ends_with($select['select'], '.' . $checkColumn)) {
				return;
			}
		}
		$this->addSelect($column);
	}

	/**
	 * distribute the select statements to the correct partition
	 *
	 * @return void
	 */
	private function applySelects(): void {
		foreach ($this->selects as $select) {
			foreach ($this->partitions as $partition) {
				if (is_string($select['select']) && $partition->isColumnInPartition($select['select'])) {
					if (isset($this->splitQueries[$partition->name])) {
						if ($select['alias']) {
							$this->splitQueries[$partition->name]->query->selectAlias($select['select'], $select['alias']);
						} else {
							$this->splitQueries[$partition->name]->query->addSelect($select['select']);
						}
						continue 2;
					}
				}
			}

			if ($select['alias']) {
				parent::selectAlias($select['select'], $select['alias']);
			} else {
				parent::addSelect($select['select']);
			}
		}
		$this->selects = [];
	}


	public function addPartition(PartitionSplit $partition): void {
		$this->partitions[] = $partition;
	}

	private function getPartition(string $table): ?PartitionSplit {
		foreach ($this->partitions as $partition) {
			if ($partition->containsTable($table) || $partition->containsAlias($table)) {
				return $partition;
			}
		}
		return null;
	}

	public function from($from, $alias = null) {
		if (is_string($from) && $partition = $this->getPartition($from)) {
			$this->mainPartition = $partition;
			if ($alias) {
				$this->mainPartition->addAlias($from, $alias);
			}
		}
		return parent::from($from, $alias);
	}

	public function innerJoin($fromAlias, $join, $alias, $condition = null): self {
		return $this->join($fromAlias, $join, $alias, $condition);
	}

	public function leftJoin($fromAlias, $join, $alias, $condition = null): self {
		return $this->join($fromAlias, $join, $alias, $condition, PartitionQuery::JOIN_MODE_LEFT);
	}

	public function join($fromAlias, $join, $alias, $condition = null, $joinMode = PartitionQuery::JOIN_MODE_INNER): self {
		$partition = $this->getPartition($join);
		$fromPartition = $this->getPartition($fromAlias);
		if ($partition && $partition !== $this->mainPartition) {
			// join from the main db to a partition
			['from' => $joinFrom, 'to' => $joinTo] = $this->splitJoinCondition($condition, $join, $alias);
			$partition->addAlias($join, $alias);
			if (!isset($this->splitQueries[$partition->name])) {
				$this->splitQueries[$partition->name] = new PartitionQuery(
					$this->newQuery(),
					$joinFrom, $joinTo,
					$joinMode
				);
				$this->splitQueries[$partition->name]->query->from($join, $alias);
				$this->ensureSelect($joinFrom);
				$this->ensureSelect($joinTo);
			} else {
				$query = $this->splitQueries[$partition->name]->query;
				if ($partition->containsAlias($fromAlias)) {
					$query->innerJoin($fromAlias, $join, $alias, $condition);
				} else {
					throw new InvalidPartitionedQueryException("Can't join across partition boundaries more than once");
				}
			}
			return $this;
		} elseif ($fromPartition && $fromPartition !== $partition) {
			// join from partition, to the main db
			['from' => $joinFrom, 'to' => $joinTo] = $this->splitJoinCondition($condition, $join, $alias);
			if (str_starts_with($fromPartition->name, 'from_')) {
				$partitionName = $fromPartition->name;
			} else {
				$partitionName = 'from_' . $fromPartition->name;
			}
			if (!isset($this->splitQueries[$partitionName])) {
				$newPartition = new PartitionSplit($partitionName, [$join]);
				$newPartition->addAlias($join, $alias);
				$this->partitions[] = $newPartition;

				$this->splitQueries[$partitionName] = new PartitionQuery(
					$this->newQuery(),
					$joinFrom, $joinTo,
					$joinMode
				);
				$this->ensureSelect($joinFrom);
				$this->ensureSelect($joinTo);
				$this->splitQueries[$partitionName]->query->from($join, $alias);
			} else {
				$fromPartition->addTable($join);
				$fromPartition->addAlias($join, $alias);

				$query = $this->splitQueries[$partitionName]->query;
				$query->innerJoin($fromAlias, $join, $alias, $condition);
			}
			return $this;
		} else {
			// join within the main db or a partition
			if ($joinMode === PartitionQuery::JOIN_MODE_INNER) {
				return parent::innerJoin($fromAlias, $join, $alias, $condition);
			} elseif ($joinMode === PartitionQuery::JOIN_MODE_LEFT) {
				return parent::leftJoin($fromAlias, $join, $alias, $condition);
			} elseif ($joinMode === PartitionQuery::JOIN_MODE_RIGHT) {
				return parent::rightJoin($fromAlias, $join, $alias, $condition);
			} else {
				throw new \InvalidArgumentException("Invalid join mode: $joinMode");
			}
		}
	}

	/**
	 * @param $condition
	 * @param string $join
	 * @param string $alias
	 * @return array{'from' => string, 'to' => string}
	 * @throws InvalidPartitionedQueryException
	 */
	private function splitJoinCondition($condition, string $join, string $alias): array {
		if ($condition === null) {
			throw new InvalidPartitionedQueryException("Can't join on $join without a condition");
		}
		$condition = str_replace('`', '', (string) $condition);
		// expect a condition in the form of 'alias1.column1 = alias2.column2'
		if (substr_count($condition, ' ') > 2) {
			throw new InvalidPartitionedQueryException("Can only join on $join with a single condition");
		}
		if (!str_contains($condition, ' = ')) {
			throw new InvalidPartitionedQueryException("Can only join on $join with an `eq` condition");
		}
		$parts = explode(' = ', $condition);
		if (str_starts_with($parts[0], "$alias.")) {
			return [
				'from' => $parts[0],
				'to' => $parts[1],
			];
		} elseif (str_starts_with($parts[1], "$alias.")) {
			return [
				'from' => $parts[1],
				'to' => $parts[0],
			];
		} else {
			throw new InvalidPartitionedQueryException("join condition for $join needs to explicitly refer to the table or alias");
		}
	}

	private function flattenPredicates(array $predicates): array {
		$result = [];
		foreach ($predicates as $predicate) {
			if ($predicate instanceof CompositeExpression && $predicate->getType() === CompositeExpression::TYPE_AND) {
				$result = array_merge($result, $this->flattenPredicates($predicate->getParts()));
			} else {
				$result[] = $predicate;
			}
		}
		return $result;
	}

	private function splitPredicatesByParts(array $predicates): array {
		$predicates = $this->flattenPredicates($predicates);

		$partitionPredicates = [];
		foreach ($predicates as $predicate) {
			$partition = $this->getPartitionForPredicate((string) $predicate);
			if ($this->mainPartition === $partition) {
				$partitionPredicates[''][] = $predicate;
			} elseif ($partition) {
				$partitionPredicates[$partition->name][] = $predicate;
			} else {
				$partitionPredicates[''][] = $predicate;
			}
		}
		return $partitionPredicates;
	}

	public function where(...$predicates) {
		foreach ($this->splitPredicatesByParts($predicates) as $alias => $predicates) {
			if ($alias !== '' && isset($this->splitQueries[$alias])) {
				$this->splitQueries[$alias]->query->where(...$predicates);
			} else {
				parent::where(...$predicates);
			}
		}
		return $this;
	}

	public function andWhere(...$where) {
		foreach ($this->splitPredicatesByParts($where) as $alias => $predicates) {
			if (isset($this->splitQueries[$alias])) {
				$this->splitQueries[$alias]->query->andWhere(...$predicates);
			} else {
				parent::andWhere(...$predicates);
			}
		}
		return $this;
	}


	private function getPartitionForPredicate(string $predicate): ?PartitionSplit {
		foreach ($this->partitions as $partition) {

			if (str_contains($predicate, '?')) {
				$this->hasPositionalParameter = true;
			}
			if ($partition->checkPredicateForTable($predicate)) {
				return $partition;
			}
		}
		return null;
	}

	public function update($update = null, $alias = null) {
		return parent::update($update, $alias);
	}

	public function insert($insert = null) {
		return parent::insert($insert);
	}

	public function delete($delete = null, $alias = null) {
		return parent::delete($delete, $alias);
	}

	public function executeQuery(?IDBConnection $connection = null): IResult {
		$this->applySelects();
		if ($this->splitQueries && $this->hasPositionalParameter) {
			throw new InvalidPartitionedQueryException("Partitioned queries aren't allowed to to positional arguments");
		}
		foreach ($this->splitQueries as $split) {
			$split->query->setParameters($this->getParameters(), $this->getParameterTypes());
		}

		$s = parent::getSQL();
		$result = parent::executeQuery($connection);
		if (count($this->splitQueries) > 0) {
			return new PartitionedResult($this->splitQueries, $result);
		} else {
			return $result;
		}
	}

	public function executeStatement(?IDBConnection $connection = null): int {
		if (count($this->splitQueries)) {
			throw new InvalidPartitionedQueryException("Partitioning write queries isn't supported");
		}
		return parent::executeStatement($connection);
	}

	public function getSQL() {
		$this->applySelects();
		return parent::getSQL();
	}

	public function getPartitionCount(): int {
		return count($this->splitQueries) + 1;
	}
}
