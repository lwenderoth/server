/**
 * SPDX-FileCopyrightText: 2024 Nextcloud GmbH and Nextcloud contributors
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
import { Folder, Permission, View, davRemoteURL, davRootPath, getNavigation } from '@nextcloud/files'
import { translate as t } from '@nextcloud/l10n'
import LinkSvg from '@mdi/svg/svg/link.svg?raw'

export default () => {
	const view = new View({
		id: 'public-file-drop',
		name: t('files_sharing', 'File drop'),
		caption: t('files_sharing', 'Upload your files to share.'),

		emptyTitle: t('files_sharing', 'File drop'),
		emptyCaption: t('files_sharing', 'You can upload your files to share'),

		icon: LinkSvg,
		order: 1,

		getContents: async () => {
			return {
				contents: [],
				// Fake a writeonly folder as root
				folder: new Folder({
					id: 0,
					source: `${davRemoteURL}${davRootPath}`,
					root: davRootPath,
					owner: null,
					permissions: Permission.CREATE,
				}),
			}
		},
	})

	const Navigation = getNavigation()
	Navigation.register(view)
}
