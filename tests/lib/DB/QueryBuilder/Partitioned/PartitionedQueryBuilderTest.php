<?php
/**
 * SPDX-FileCopyrightText: 2024 Nextcloud GmbH and Nextcloud contributors
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

declare(strict_types=1);

namespace Test\DB\QueryBuilder\Partitioned;

use OC\DB\QueryBuilder\Partitioned\PartitionSplit;
use OC\DB\QueryBuilder\Partitioned\PartitionedQueryBuilder;
use OC\SystemConfig;
use OCP\DB\QueryBuilder\IQueryBuilder;
use OCP\IDBConnection;
use OCP\Server;
use Psr\Log\LoggerInterface;
use Test\TestCase;

/**
 * @group DB
 */
class PartitionedQueryBuilderTest extends TestCase {
	private IDBConnection $connection;
	private SystemConfig $systemConfig;
	private LoggerInterface $logger;

	protected function setUp(): void {
		$this->connection = Server::get(IDBConnection::class);
		$this->systemConfig = Server::get(SystemConfig::class);
		$this->logger = Server::get(LoggerInterface::class);
	}

	protected function tearDown(): void {
		$this->cleanupDb();
		parent::tearDown();
	}


	private function getQueryBuilder(): PartitionedQueryBuilder {
		return new PartitionedQueryBuilder($this->connection, $this->systemConfig, $this->logger, []);
	}

	private function setupFileCache() {
		$query = $this->connection->getQueryBuilder();
		$query->insert('filecache')
			->values([
				'storage' => $query->createNamedParameter(1001001, IQueryBuilder::PARAM_INT),
				'path' => $query->createNamedParameter('file1'),
				'path_hash' => $query->createNamedParameter(md5('file1')),
			]);
		$query->executeStatement();
		$fileId = $query->getLastInsertId();

		$query = $this->connection->getQueryBuilder();
		$query->insert('filecache_extended')
			->hintShardKey('storage', 1001001)
			->values([
				'fileid' => $query->createNamedParameter($fileId, IQueryBuilder::PARAM_INT),
				'upload_time' => $query->createNamedParameter(1234, IQueryBuilder::PARAM_INT),
			]);
		$query->executeStatement();

		$query = $this->connection->getQueryBuilder();
		$query->insert('mounts')
			->values([
				'storage_id' => $query->createNamedParameter(1001001, IQueryBuilder::PARAM_INT),
				'user_id' => $query->createNamedParameter('partitioned_test'),
				'mount_point' => $query->createNamedParameter('/mount/point'),
				'mount_provider_class' => $query->createNamedParameter('test'),
				'root_id' => $query->createNamedParameter($fileId, IQueryBuilder::PARAM_INT),
			]);
		$query->executeStatement();
	}

	private function cleanupDb() {
		$query = $this->connection->getQueryBuilder();
		$query->delete('filecache')
			->where($query->expr()->gt('storage', $query->createNamedParameter(1000000, IQueryBuilder::PARAM_INT)));
		$query->executeStatement();

		$query = $this->connection->getQueryBuilder();
		$query->delete('filecache_extended');
		$query->executeStatement();

		$query = $this->connection->getQueryBuilder();
		$query->delete('mounts')
			->where($query->expr()->like('user_id', $query->createNamedParameter('partitioned_%')));
		$query->executeStatement();
	}

	public function testSimpleOnlyPartitionQuery() {
		$this->setupFileCache();
		$builder = $this->getQueryBuilder();
		$builder->addPartition(new PartitionSplit('filecache', ['filecache']));

		// query borrowed from UserMountCache
		$query = $builder->select('path')
			->from('filecache')
			->where($builder->expr()->eq('storage', $builder->createPositionalParameter(1001001, IQueryBuilder::PARAM_INT)));

		$results = $query->executeQuery()->fetchAll();
		$this->assertCount(1, $results);
		$this->assertEquals($results[0]['path'], 'file1');
	}

	public function testSimplePartitionedQuery() {
		$this->setupFileCache();
		$builder = $this->getQueryBuilder();
		$builder->addPartition(new PartitionSplit('filecache', ['filecache']));

		// query borrowed from UserMountCache
		$query = $builder->select('storage_id', 'root_id', 'user_id', 'mount_point', 'mount_id', 'f.path', 'mount_provider_class')
			->from('mounts', 'm')
			->innerJoin('m', 'filecache', 'f', $builder->expr()->eq('m.root_id', 'f.fileid'))
			->where($builder->expr()->eq('storage_id', $builder->createPositionalParameter(1001001, IQueryBuilder::PARAM_INT)));

		$query->andWhere($builder->expr()->eq('user_id', $builder->createPositionalParameter('partitioned_test')));

		$results = $query->executeQuery()->fetchAll();
		$this->assertCount(1, $results);
		$this->assertEquals($results[0]['user_id'], 'partitioned_test');
		$this->assertEquals($results[0]['mount_point'], '/mount/point');
		$this->assertEquals($results[0]['mount_provider_class'], 'test');
		$this->assertEquals($results[0]['path'], 'file1');
	}

	public function testMultiTablePartitionedQuery() {
		$this->setupFileCache();
		$builder = $this->getQueryBuilder();
		$builder->addPartition(new PartitionSplit('filecache', ['filecache', 'filecache_extended']));

		$query = $builder->select('storage_id', 'root_id', 'user_id', 'mount_point', 'mount_id', 'f.path', 'mount_provider_class', 'fe.upload_time')
			->from('mounts', 'm')
			->innerJoin('m', 'filecache', 'f', $builder->expr()->eq('m.root_id', 'f.fileid'))
			->innerJoin('f', 'filecache_extended', 'fe', $builder->expr()->eq('f.fileid', 'fe.fileid'))
			->where($builder->expr()->eq('storage_id', $builder->createPositionalParameter(1001001, IQueryBuilder::PARAM_INT)));

		$query->andWhere($builder->expr()->eq('user_id', $builder->createPositionalParameter('partitioned_test')));

		$results = $query->executeQuery()->fetchAll();
		$this->assertCount(1, $results);
		$this->assertEquals($results[0]['user_id'], 'partitioned_test');
		$this->assertEquals($results[0]['mount_point'], '/mount/point');
		$this->assertEquals($results[0]['mount_provider_class'], 'test');
		$this->assertEquals($results[0]['path'], 'file1');
		$this->assertEquals($results[0]['upload_time'], 1234);
	}
}
