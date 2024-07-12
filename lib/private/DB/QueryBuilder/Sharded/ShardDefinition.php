<?php

declare(strict_types=1);
/**
 * SPDX-FileCopyrightText: 2024 Robin Appelman <robin@icewind.nl>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace OC\DB\QueryBuilder\Sharded;

use OC\Config;
use OCP\DB\QueryBuilder\Sharded\IShardMapper;

class ShardDefinition {
	/**
	 * @param string $table
	 * @param string $primaryKey
	 * @param string $shardKey
	 * @param string[] $companionTables
	 * @param array $shards
	 * @param IShardMapper $shardMapper
	 */
	public function __construct(
		public string $table,
		public string $primaryKey,
		public string $shardKey,
		public IShardMapper $shardMapper,
		public array $companionTables = [],
		public array $shards = [],
	) {
	}

	public function hasTable(string $table): bool {
		if ($this->table === $table) {
			return true;
		}
		return in_array($table, $this->companionTables);
	}

	public function getShardForKey(int $key): int {
		return $this->shardMapper->getShardForKey($key, count($this->shards));
	}

	public function getAllShards(): array {
		return range(0, count($this->shards) - 1);
	}
}
