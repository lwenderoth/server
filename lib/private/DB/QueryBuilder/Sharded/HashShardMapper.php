<?php

declare(strict_types=1);
/**
 * SPDX-FileCopyrightText: 2024 Robin Appelman <robin@icewind.nl>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace OC\DB\QueryBuilder\Sharded;

use OCP\DB\QueryBuilder\Sharded\IShardMapper;

/**
 * Map string key to an int-range by hashing the key
 */
class HashShardMapper implements IShardMapper {
	public function __construct(
		private array $shardCounts
	) {
	}

	public function getShardForKey(string $table, string $key): int {
		$count = $this->shardCounts[$table] ?? 1;
		$int = unpack('L', substr(md5($key, true), 0, 4))[1];
		return $int % $count;
	}
}
