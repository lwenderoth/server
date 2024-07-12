<?php

declare(strict_types=1);
/**
 * SPDX-FileCopyrightText: 2024 Robin Appelman <robin@icewind.nl>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace OC\DB\QueryBuilder\Sharded;

use OC\DB\ConnectionAdapter;
use OC\DB\QueryBuilder\CompositeExpression;
use OC\DB\QueryBuilder\Parameter;
use OC\DB\QueryBuilder\QueryBuilder;
use OC\DB\ArrayResult;
use OC\SystemConfig;
use OCP\DB\IResult;
use OCP\IDBConnection;
use Psr\Log\LoggerInterface;

class ShardedQueryBuilder extends QueryBuilder {
	private array $shardKeys = [];
	private array $primaryKeys = [];
	private ?ShardDefinition $shardDefinition = null;
	/** @var bool Run the query across all shards */
	private bool $allShards = false;
	private ?string $insertTable = null;
	private mixed $lastInsertId = null;
	private ?IDBConnection $lastInsertConnection = null;

	/**
	 * @param ConnectionAdapter $connection
	 * @param SystemConfig $systemConfig
	 * @param LoggerInterface $logger
	 * @param ShardDefinition[] $shardDefinitions
	 */
	public function __construct(
		ConnectionAdapter              $connection,
		SystemConfig                   $systemConfig,
		LoggerInterface                $logger,
		private array                  $shardDefinitions,
		private ShardConnectionManager $shardConnectionManager,
	) {
		parent::__construct($connection, $systemConfig, $logger);
	}

	public function getShardKeys(): array {
		return $this->getKeyValues($this->shardKeys);
	}

	public function getPrimaryKeys(): array {
		return $this->getKeyValues($this->primaryKeys);
	}

	private function getKeyValues(array $keys): array {
		$values = [];
		foreach ($keys as $key) {
			$values = array_merge($values, $this->getKeyValue($key));
		}
		return array_values(array_unique($values));
	}

	private function getKeyValue($value): array {
		if ($value instanceof Parameter) {
			$value = (string)$value;
		}
		if (is_string($value) && str_starts_with($value, ':')) {
			$param = $this->getParameter(substr($value, 1));
			if (is_array($param)) {
				return $param;
			} else {
				return [$param];
			}
		} elseif ($value !== null) {
			return [$value];
		} else {
			return [];
		}
	}

	public function where(...$predicates) {
		return $this->andWhere(...$predicates);
	}

	public function andWhere(...$where) {
		if ($where) {
			foreach ($where as $predicate) {
				$this->tryLoadShardKey($predicate);
			}
			parent::andWhere(...$where);
		}
		return $this;
	}

	private function tryLoadShardKey($predicate): void {
		if (!$this->shardDefinition) {
			return;
		}
		if ($keys = $this->tryExtractShardKeys($predicate, $this->shardDefinition->shardKey)) {
			$this->shardKeys += $keys;
		}
		if ($keys = $this->tryExtractShardKeys($predicate, $this->shardDefinition->primaryKey)) {
			$this->primaryKeys += $keys;
		}
	}

	/**
	 * @param $predicate
	 * @param string $column
	 * @return string[]
	 */
	private function tryExtractShardKeys($predicate, string $column): array {
		if ($predicate instanceof CompositeExpression) {
			$values = [];
			foreach ($predicate->getParts() as $part) {
				$partValues = $this->tryExtractShardKeys($part, $column);
				// for OR expressions, we can only rely on the predicate if all parts contain the comparison
				if ($predicate->getType() === CompositeExpression::TYPE_OR && !$partValues) {
					return [];
				}
				$values = array_merge($values, $partValues);
			}
			return $values;
		}
		$predicate = (string)$predicate;
		// expect a condition in the form of 'alias1.column1 = placeholder' or 'alias1.column1 in placeholder'
		if (substr_count($predicate, ' ') > 2) {
			return [];
		}
		if (str_contains($predicate, ' = ')) {
			$parts = explode(' = ', $predicate);
			if ($parts[0] === "`{$column}`" || str_ends_with($parts[0], "`.`{$column}`")) {
				return [$parts[1]];
			} else {
				return [];
			}
		}

		if (str_contains($predicate, ' IN ')) {
			$parts = explode(' IN ', $predicate);
			if ($parts[0] === "`{$column}`" || str_ends_with($parts[0], "`.`{$column}`")) {
				return [trim(trim($parts[1], '('), ')')];
			} else {
				return [];
			}
		}

		return [];
	}

	public function set($key, $value) {
		if ($this->shardDefinition && $key === $this->shardDefinition->shardKey) {
			throw new InvalidShardedQueryException("Changing the sharding key with an update isn't allowed");
		}
		return parent::set($key, $value);
	}

	public function setValue($column, $value) {
		if ($this->shardDefinition) {
			if ($column === $this->shardDefinition->primaryKey) {
				$this->primaryKeys[] = $value;
			}
			if ($column === $this->shardDefinition->shardKey) {
				$this->shardKeys[] = $value;
			}
		}
		return parent::setValue($column, $value);
	}

	public function values(array $values) {
		foreach ($values as $column => $value) {
			$this->setValue($column, $value);
		}
		return $this;
	}

	private function actOnTable(string $table): void {
		foreach ($this->shardDefinitions as $shardDefinition) {
			if ($shardDefinition->hasTable($table)) {
				$this->shardDefinition = $shardDefinition;
			}
		}
	}

	public function from($from, $alias = null) {
		if (is_string($from) && $from) {
			$this->actOnTable($from);
		}
		return parent::from($from, $alias);
	}

	public function update($update = null, $alias = null) {
		if (is_string($update) && $update) {
			$this->actOnTable($update);
		}
		return parent::update($update, $alias);
	}

	public function insert($insert = null) {
		if (is_string($insert) && $insert) {
			$this->insertTable = $insert;
			$this->actOnTable($insert);
		}
		return parent::insert($insert);
	}

	public function delete($delete = null, $alias = null) {
		if (is_string($delete) && $delete) {
			$this->actOnTable($delete);
		}
		return parent::delete($delete, $alias);
	}

	private function checkJoin(string $table): void {
		if ($this->shardDefinition) {
			if (!$this->shardDefinition->hasTable($table)) {
				throw new InvalidShardedQueryException("Sharded query on {$this->shardDefinition->table} isn't allowed to join on $table");
			}
		}
	}

	public function innerJoin($fromAlias, $join, $alias, $condition = null) {
		$this->checkJoin($join);
		return parent::innerJoin($fromAlias, $join, $alias, $condition);
	}

	public function leftJoin($fromAlias, $join, $alias, $condition = null) {
		$this->checkJoin($join);
		return parent::leftJoin($fromAlias, $join, $alias, $condition);
	}

	public function rightJoin($fromAlias, $join, $alias, $condition = null) {
		if ($this->shardDefinition) {
			throw new InvalidShardedQueryException("Sharded query on {$this->shardDefinition->table} isn't allowed to right join");
		}
		return parent::rightJoin($fromAlias, $join, $alias, $condition);
	}

	public function join($fromAlias, $join, $alias, $condition = null) {
		return $this->innerJoin($fromAlias, $join, $alias, $condition);
	}

	public function hintShardKey(string $column, mixed $value) {
		if ($column === $this->shardDefinition?->primaryKey) {
			$this->primaryKeys[] = $value;
		}
		if ($column === $this->shardDefinition?->shardKey) {
			$this->shardKeys[] = $value;
		}
		return $this;
	}

	public function runAcrossAllShards() {
		$this->allShards = true;
		return $this;
	}

	/**
	 * @throws InvalidShardedQueryException
	 */
	public function validate(): void {
		if ($this->shardDefinition && $this->insertTable) {
			if ($this->allShards) {
				throw new InvalidShardedQueryException("Can't insert across all shards");
			}
			if (empty($this->getShardKeys())) {
				throw new InvalidShardedQueryException("Can't insert without shard key");
			}
		}
		if ($this->shardDefinition && !$this->allShards) {
			if (empty($this->getShardKeys()) && empty($this->getPrimaryKeys())) {
				throw new InvalidShardedQueryException("No shard key or primary key set for query");
			}
		}
	}

	/**
	 * @return int[]
	 */
	private function getShards(): array {
		if ($this->allShards) {
			return $this->shardDefinition->getAllShards();
		}
		$shardKeys = $this->getShardKeys();
		if (empty($shardKeys)) {
			// todo: get shard keys from cache by primary keys
			return $this->shardDefinition->getAllShards();
		}
		$shards = array_map(function ($shardKey) {
			return $this->shardDefinition->getShardForKey((int)$shardKey);
		}, $shardKeys);
		return array_values(array_unique($shards));
	}

	public function executeQuery(?IDBConnection $connection = null): IResult {
		$this->validate();
		if ($this->shardDefinition) {
			$shards = $this->getShards();
			if (count($shards) === 1) {
				return parent::executeQuery($this->shardConnectionManager->getConnection($this->shardDefinition, $shards[0]));
			} else {
				$results = [];
				foreach ($shards as $shard) {
					$shardConnection = $this->shardConnectionManager->getConnection($this->shardDefinition, $shard);
					$subResult = parent::executeQuery($shardConnection);
					$results = array_merge($results, $subResult->fetchAll());
					$subResult->closeCursor();
				}
				return new ArrayResult($results);
			}
		}
		return parent::executeQuery($connection);
	}

	public function executeStatement(?IDBConnection $connection = null): int {
		$this->validate();
		if ($this->shardDefinition) {
			$shards = $this->getShards();
			$count = 0;
			foreach ($shards as $shard) {
				$shardConnection = $this->shardConnectionManager->getConnection($this->shardDefinition, $shard);
				if (!$this->primaryKeys && $this->shardDefinition->table === $this->insertTable) {
					// todo: is random primary key fine, or do we need to do shared-autoincrement
					/**
					 * atomic autoincrement:
					 *
					 * $next = $cache->inc('..');
					 * if (!$next) {
					 *     $last = $this->getMaxValue();
					 *	   $success = $cache->add('..', $last + 1);
					 *	   if ($success) {
					 *	       return $last + 1;
					 *	   } else {
					 * 		   / somebody else set it
					 *	       return $cache->inc('..');
					 *	   }
					 * } else {
					 *     return $next
					 * }
					 */
					$id = random_int(0, PHP_INT_MAX);
					$this->setValue($this->shardDefinition->primaryKey, $this->createNamedParameter($id, self::PARAM_INT));
					$this->lastInsertId = $id;
				}
				$count += parent::executeStatement($shardConnection);

				if ($this->insertTable) {
					$this->lastInsertConnection = $shardConnection;
				}
			}
			return $count;
		}
		return parent::executeStatement($connection);
	}

	public function getLastInsertId(): int {
		if ($this->lastInsertId) {
			return $this->lastInsertId;
		}
		if ($this->lastInsertConnection) {
			$table = $this->builder->prefixTableName($this->insertTable);
			return $this->lastInsertConnection->lastInsertId($table);
		} else {
			return parent::getLastInsertId();
		}
	}


}
