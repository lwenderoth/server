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

namespace OC\DB\QueryBuilder\Sharded;

use OC\DB\QueryBuilder\Partitioned\PartitionQuery;
use OCP\DB\IResult;
use PDO;

class ShardedResult implements IResult {
	public function __construct(
		private array $rows,
	) {
	}

	public function closeCursor(): bool {
		// noop
		return true;
	}

	public function fetch(int $fetchMode = PDO::FETCH_ASSOC) {
		$row = array_shift($this->rows);
		if (!$row) {
			return false;
		}
		return match ($fetchMode) {
			PDO::FETCH_ASSOC => $row,
			PDO::FETCH_NUM => array_values($row),
			default => throw new InvalidShardedQueryException("Fetch mode not supported for sharded queries"),
		};

	}

	public function fetchAll(int $fetchMode = PDO::FETCH_ASSOC): array {
		return match ($fetchMode) {
			PDO::FETCH_ASSOC => $this->rows,
			PDO::FETCH_NUM => array_map(function ($row) {
				return array_values($row);
			}, $this->rows),
			default => throw new InvalidShardedQueryException("Fetch mode not supported for sharded queries"),
		};
	}

	public function fetchColumn() {
		return $this->fetchOne();
	}

	public function fetchOne() {
		$row = $this->fetch();
		if ($row) {
			return current($row);
		} else {
			return false;
		}
	}

	public function rowCount(): int {
		return count($this->rows);
	}
}
