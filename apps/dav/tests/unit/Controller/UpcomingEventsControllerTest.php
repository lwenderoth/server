<?php declare(strict_types=1);

/**
 * SPDX-FileCopyrightText: 2024 Nextcloud GmbH and Nextcloud contributors
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace OCA\DAV\Tests\Unit\DAV\Controller;

use DateTimeImmutable;
use OCA\DAV\Controller\UpcomingEventsController;
use OCP\AppFramework\Utility\ITimeFactory;
use OCP\Calendar\ICalendarQuery;
use OCP\Calendar\IManager;
use OCP\IRequest;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class UpcomingEventsControllerTest extends TestCase {

	private IRequest|MockObject $request;
	private MockObject|IManager $calendarManager;
	private ITimeFactory|MockObject $timeFactory;

	protected function setUp(): void {
		parent::setUp();

		$this->request = $this->createMock(IRequest::class);
		$this->calendarManager = $this->createMock(IManager::class);
		$this->timeFactory = $this->createMock(ITimeFactory::class);
	}

	public function testGetEventsAnonymously() {
		$this->controller = new UpcomingEventsController(
			$this->request,
			null,
			$this->calendarManager,
			$this->timeFactory,
		);

		$response = $this->controller->getEvents('https://cloud.example.com/call/123');

		self::assertNull($response->getData());
		self::assertSame(401, $response->getStatus());
	}

	public function testGetEventsByLocation() {
		$now = new DateTimeImmutable('2024-07-08T18:20:20Z');
		$this->timeFactory->method('now')
			->willReturn($now);
		$query = $this->createMock(ICalendarQuery::class);
		$this->calendarManager->method('newQuery')
			->with('principals/users/u1')
			->willReturn($query);
		$query->expects(self::once())
			->method('addSearchProperty')
			->with('LOCATION');
		$query->expects(self::once())
			->method('setSearchPattern')
			->with('https://cloud.example.com/call/123');
		$this->calendarManager->expects(self::once())
			->method('searchForPrincipal')
			->with($query)
			->willReturn([
				[
					'uri' => 'ev1',
					'calendar-key' => '1',
					'objects' => [
						0 => [
							'DTSTART' => [
								new DateTimeImmutable('now'),
							],
						],
					],
				],
			]);
		$this->controller = new UpcomingEventsController(
			$this->request,
			'u1',
			$this->calendarManager,
			$this->timeFactory,
		);

		$response = $this->controller->getEvents('https://cloud.example.com/call/123');

		self::assertNotNull($response->getData());
		self::assertIsArray($response->getData());
		self::assertCount(1, $response->getData()['events']);
		self::assertSame(200, $response->getStatus());
		$event1 = $response->getData()['events'][0];
		self::assertEquals('ev1', $event1['uri']);
	}
}
