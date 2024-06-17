<?php

declare(strict_types=1);
/**
 * SPDX-FileCopyrightText: 2024 Robin Appelman <robin@icewind.nl>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace OC\DB\QueryBuilder\Sharded;

class ShardDefinition {
	/**
	 * @param string $table
	 * @param string $primaryKey
	 * @param string $shardKey
	 * @param string[] $companionTables
	 */
	public function __construct(
		public string $table,
		public string $primaryKey,
		public string $shardKey,
		public array $companionTables = [],
	) {
	}

	public function hasTable(string $table): bool {
		if ($this->table === $table) {
			return true;
		}
		return in_array($table, $this->companionTables);
	}
}
