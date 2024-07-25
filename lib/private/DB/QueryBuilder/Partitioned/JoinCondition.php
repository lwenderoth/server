<?php

declare(strict_types=1);
/**
 * SPDX-FileCopyrightText: 2024 Robin Appelman <robin@icewind.nl>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace OC\DB\QueryBuilder\Partitioned;

use OC\DB\QueryBuilder\CompositeExpression;

class JoinCondition {
	public function __construct(
		public string $fromColumn,
		public string $toColumn,
		public array $fromConditions,
		public array $toConditions,
	) {
	}

	/**
	 * @param JoinCondition[] $conditions
	 * @return JoinCondition
	 */
	public static function merge(array $conditions): JoinCondition {
		$fromColumn = "";
		$toColumn = "";
		$fromConditions = [];
		$toConditions = [];
		foreach ($conditions as $condition) {
			if (($condition->fromColumn && $fromColumn) || ($condition->toColumn && $toColumn)) {
				throw new InvalidPartitionedQueryException("Can't join from {$condition->fromColumn} to {$condition->toColumn} as it already join froms {$fromColumn} to {$toColumn}");
			}
			if ($condition->fromColumn) {
				$fromColumn = $condition->fromColumn;
			}
			if ($condition->toColumn) {
				$toColumn = $condition->toColumn;
			}
			$fromConditions = array_merge($fromConditions, $condition->fromConditions);
			$toConditions = array_merge($toConditions, $condition->toConditions);
		}
		return new JoinCondition($fromColumn, $toColumn, $fromConditions, $toConditions);
	}

	/**
	 * @param null|string|CompositeExpression $condition
	 * @param string $join
	 * @param string $alias
	 * @param string $fromAlias
	 * @return JoinCondition
	 * @throws InvalidPartitionedQueryException
	 */
	public static function parse($condition, string $join, string $alias, string $fromAlias): JoinCondition {
		if ($condition === null) {
			throw new InvalidPartitionedQueryException("Can't join on $join without a condition");
		}

		$result = self::parseSubCondition($condition, $join, $alias, $fromAlias);
		if (!$result->fromColumn || !$result->toColumn) {
			throw new InvalidPartitionedQueryException("No join condition found from $fromAlias to $alias");
		}
		return $result;
	}

	private static function parseSubCondition($condition, string $join, string $alias, string $fromAlias): JoinCondition {
		if ($condition instanceof CompositeExpression) {
			if ($condition->getType() === CompositeExpression::TYPE_OR) {
				throw new InvalidPartitionedQueryException("Can only join on $join with an OR expression");
			}
			return self::merge(array_map(function ($subCondition) use ($join, $alias, $fromAlias) {
				return self::parseSubCondition($subCondition, $join, $alias, $fromAlias);
			}, $condition->getParts()));
		}

		$condition = (string)$condition;
		$isSubCondition = self::isExtraCondition($condition);
		if ($isSubCondition) {
			if (self::mentionsAlias($condition, $fromAlias)) {
				return new JoinCondition("", "", [$condition], []);
			} else {
				return new JoinCondition("", "", [], [$condition]);
			}
		}

		$condition = str_replace('`', '', $condition);

		// expect a condition in the form of 'alias1.column1 = alias2.column2'
		if (substr_count($condition, ' ') > 2) {
			throw new InvalidPartitionedQueryException("Can only join on $join with a single condition");
		}
		if (!str_contains($condition, ' = ')) {
			throw new InvalidPartitionedQueryException("Can only join on $join with an `eq` condition");
		}
		$parts = explode(' = ', $condition);
		if (self::mentionsAlias($parts[0], $fromAlias)) {
			return new JoinCondition($parts[0], $parts[1], [], []);
		} elseif (self::mentionsAlias($parts[1], $fromAlias)) {
			return new JoinCondition($parts[1], $parts[0], [], []);
		} else {
			throw new InvalidPartitionedQueryException("join condition for $join needs to explicitly refer to the table by alias");
		}
	}

	/**
	 * Check that a condition is an extra limit on the from/to part, and not the join condition
	 *
	 * This is done by checking that only one of the halves of the condition references a column
	 */
	private static function isExtraCondition(string $condition): bool {
		$parts = explode(' ', $condition, 2);
		return str_contains($parts[0], '`') xor str_contains($parts[1], '`');
	}

	private static function mentionsAlias(string $condition, string $alias): bool {
		return str_contains($condition, "$alias.");
	}
}
