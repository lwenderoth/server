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

use OC\DB\QueryBuilder\QueryBuilder;
use OC\DB\QueryBuilder\Sharded\ShardedQueryBuilder;
use OCP\DB\IResult;

class PartitionedQueryBuilder extends ShardedQueryBuilder {
	/** @var array<string, PartitionQuery> $splitQueries */
	private array $splitQueries = [];
	/** @var list<PartitionSplit> */
	private array $partitions = [];

	/** @var string[] */
	private array $selects = [];
	/** @var array{'column' => string, 'alias' => string}[] */
	private array $selectAliases = [];
	private bool $isWrite = false;

	// we need to save selects until we know all the table aliases
	public function select(...$selects) {
		$this->selects = $selects;
		return $this;
	}

	public function addSelect(...$selects) {
		$this->selects = array_merge($this->selects, $selects);
		return $this;
	}

	public function selectAlias($select, $alias) {
		$this->selectAliases[] = ['select' => $select, 'alias' => $alias];
		return $this;
	}

	private function applySelects(): void {
		// ensure that all join columns are selected
		foreach ($this->splitQueries as $split) {
			$joinColumn = $split->joinToColumn;
			if (str_contains($joinColumn, '.')) {
				[, $joinColumn] = explode('.', $joinColumn);
			}
			foreach ($this->selects as $select) {
				if ($select === $joinColumn || str_ends_with($select, '.' . $joinColumn)) {
					continue 2;
				}
			}
			$this->selects[] = $split->joinToColumn;
		}

		foreach ($this->selects as $select) {
			foreach ($this->partitions as $partition) {
				if (is_string($select) && $partition->isColumnInPartition($select)) {
					if (isset($this->splitQueries[$partition->name])) {
						$this->splitQueries[$partition->name]->query->addSelect($select);
						continue 2;
					}
				}
			}
			parent::addSelect($select);
		}
		$this->selects = [];
		foreach ($this->selectAliases as $select) {
			foreach ($this->partitions as $partition) {
				if (is_string($select['select']) && $partition->isColumnInPartition($select['select'])) {
					if (isset($this->splitQueries[$partition->name])) {
						$this->splitQueries[$partition->name]->query->selectAlias($select['select'], $select['alias']);
						continue 2;
					}
				}
			}
			parent::selectAlias($select['select'], $select['alias']);
		}
		$this->selectAliases = [];
	}

	public function addPartition(PartitionSplit $partition): void {
		$this->partitions[] = $partition;
	}

	private function getPartition(string $table): ?PartitionSplit {
		foreach ($this->partitions as $partition) {
			if ($partition->containsTable($table)) {
				return $partition;
			}
		}
		return null;
	}

	public function innerJoin($fromAlias, $join, $alias, $condition = null): self {
		if ($partition = $this->getPartition($join)) {
			['from' => $joinFrom, 'to' => $joinTo] = $this->splitJoinCondition($condition, $join, $alias);
			$partition->addAlias($join, $alias);
			if (!isset($this->splitQueries[$partition->name])) {
				$this->splitQueries[$partition->name] = new PartitionQuery(
					$this->getConnection()->getQueryBuilder(),
					$joinFrom, $joinTo,
					PartitionQuery::JOIN_MODE_INNER
				);
				$this->splitQueries[$partition->name]->query->from($join, $alias);
			} else {
				$query = $this->splitQueries[$partition->name]->query;
				if ($partition->containsAlias($fromAlias)) {
					$query->innerJoin($fromAlias, $join, $alias, $condition);
				} else {
					throw new InvalidPartitionedQueryException("Can't join across partition boundaries more than once");
				}
			}
			return $this;
		} else {
			return parent::innerJoin($fromAlias, $join, $alias, $condition);
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

	private function splitPredicatesByParts(array $predicates): array {
		$partitionPredicates = [];
		foreach ($predicates as $predicate) {
			$partition = $this->getPartitionForPredicate((string) $predicate);
			if ($partition) {
				$partitionPredicates[$partition->name][] = $predicate;
			} else {
				$partitionPredicates[''][] = $predicate;
			}
		}
		return $partitionPredicates;
	}

	public function where(...$predicates) {
		foreach ($this->splitPredicatesByParts($predicates) as $alias => $predicates) {
			if ($alias === '') {
				parent::where(...$predicates);
			} else {
				$this->splitQueries[$alias]->query->where(...$predicates);
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
			if ($partition->checkPredicateForTable($predicate)) {
				return $partition;
			}
		}
		return null;
	}

	public function update($update = null, $alias = null) {
		$this->isWrite = true;
		return parent::update($update, $alias);
	}

	public function insert($insert = null) {
		$this->isWrite = true;
		return parent::insert($insert);
	}

	public function delete($delete = null, $alias = null) {
		$this->isWrite = true;
		return parent::delete($delete, $alias);
	}

	public function execute() {
		if ($this->isWrite) {
			if (count($this->splitQueries)) {
				throw new InvalidPartitionedQueryException("Partitioning write queries isn't supported");
			}
		} else {
			$this->applySelects();
			foreach ($this->splitQueries as $split) {
				$split->query->setParameters($this->getParameters(), $this->getParameterTypes());
			}
		}
		$result = parent::execute();
		if ($result instanceof IResult && count($this->splitQueries) > 0) {
			return new PartitionedResult($this->splitQueries, $result);
		} else {
			return $result;
		}
	}

	public function getSQL() {
		$this->applySelects();
		return parent::getSQL();
	}
}
