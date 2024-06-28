/**
 * SPDX-FileCopyrightText: 2024 Nextcloud GmbH and Nextcloud contributors
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
import type { VueConstructor } from 'vue'

import { Folder, Header, registerFileListHeaders } from '@nextcloud/files'
import Vue from 'vue'

/**
 * Register the  "note to recipient" as a files list header
 */
export default function registerNoteToRecipient() {
	let FilesHeaderNoteToRecipient: VueConstructor
	let instance

	registerFileListHeaders(new Header({
		id: 'note-to-recipient',
		order: 0,
		// Always if there is a note
		enabled: (folder: Folder) => Boolean(folder.attributes.note),
		// No need to update the note does not change
		updated: (folder: Folder) => {
			instance.updateFolder(folder)
		},
		// render simply spawns the component
		render: async (el: HTMLElement, folder: Folder) => {
			if (FilesHeaderNoteToRecipient === undefined) {
				const { default: component } = await import('../views/FilesHeaderNoteToRecipient.vue')
				FilesHeaderNoteToRecipient = Vue.extend(component)
			}
			instance = new FilesHeaderNoteToRecipient().$mount(el)
			instance.updateFolder(folder)
		},
	}))
}
