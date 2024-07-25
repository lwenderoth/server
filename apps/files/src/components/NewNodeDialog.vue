<!--
  - @copyright Copyright (c) 2024 Ferdinand Thiessen <opensource@fthiessen.de>
  -
  - @author Ferdinand Thiessen <opensource@fthiessen.de>
  -
  - @license AGPL-3.0-or-later
  -
  - This program is free software: you can redistribute it and/or modify
  - it under the terms of the GNU Affero General Public License as
  - published by the Free Software Foundation, either version 3 of the
  - License, or (at your option) any later version.
  -
  - This program is distributed in the hope that it will be useful,
  - but WITHOUT ANY WARRANTY; without even the implied warranty of
  - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  - GNU Affero General Public License for more details.
  -
  - You should have received a copy of the GNU Affero General Public License
  - along with this program. If not, see <http://www.gnu.org/licenses/>.
  -
  -->
<template>
	<NcDialog :name="name"
		:open="open"
		close-on-click-outside
		out-transition
		@update:open="emit('close', null)">
		<template #actions>
			<NcButton type="primary"
				:disabled="!isUniqueName"
				@click="onCreate">
				{{ t('files', 'Create') }}
			</NcButton>
		</template>
		<form @submit.prevent="onCreate">
			<NcTextField ref="input"
				class="dialog__input"
				:error="!isUniqueName"
				:helper-text="errorMessage"
				:label="label"
				:value.sync="localDefaultName" />
		</form>
	</NcDialog>
</template>

<script setup lang="ts">
import type { ComponentPublicInstance, PropType } from 'vue'
import { getUniqueName } from '@nextcloud/files'
import { t } from '@nextcloud/l10n'
import { extname } from 'path'
import { nextTick, onMounted, ref, watch, watchEffect } from 'vue'
import { getFilenameValidity } from '../utils/filenameValidity.ts'

import NcButton from '@nextcloud/vue/dist/Components/NcButton.js'
import NcDialog from '@nextcloud/vue/dist/Components/NcDialog.js'
import NcTextField from '@nextcloud/vue/dist/Components/NcTextField.js'

const props = defineProps({
	/**
	 * The name to be used by default
	 */
	defaultName: {
		type: String,
		default: t('files', 'New folder'),
	},
	/**
	 * Other files that are in the current directory
	 */
	otherNames: {
		type: Array as PropType<string[]>,
		default: () => [],
	},
	/**
	 * Open state of the dialog
	 */
	open: {
		type: Boolean,
		default: true,
	},
	/**
	 * Dialog name
	 */
	name: {
		type: String,
		default: t('files', 'Create new folder'),
	},
	/**
	 * Input label
	 */
	label: {
		type: String,
		default: t('files', 'Folder name'),
	},
})

const emit = defineEmits<{
	(event: 'close', name: string | null): void
}>()

const localDefaultName = ref<string>(props.defaultName)
const nameInput = ref<ComponentPublicInstance>()
const formElement = ref<HTMLFormElement>()
const validity = ref('')

/**
 * Focus the filename input field
 */
function focusInput() {
	nextTick(() => {
		// get the input element
		const input = nameInput.value?.$el.querySelector('input')
		if (!props.open || !input) {
			return
		}

		// length of the basename
		const length = localDefaultName.value.length - extname(localDefaultName.value).length
		// focus the input
		input.focus()
		// and set the selection to the basename (name without extension)
		input.setSelectionRange(0, length)
	})
}

const forbiddenCharacters = loadState<string>('files', 'forbiddenCharacters', '').split('')

// Reset local name on props change
watch(() => props.defaultName, () => {
	localDefaultName.value = getUniqueName(props.defaultName, props.otherNames)
})

// Validate the local name
watchEffect(() => {
	if (props.otherNames.includes(localDefaultName.value)) {
		validity.value = t('files', 'This name is already in use.')
	} else {
		validity.value = getFilenameValidity(localDefaultName.value)
	}
	const input = nameInput.value?.$el.querySelector('input')
	if (input) {
		input.setCustomValidity(validity.value)
		input.reportValidity()
	}
})

// Ensure the input is focussed even if the dialog is already mounted but not open
watch(() => props.open, () => {
	nextTick(() => {
		focusInput()
	})
})

onMounted(() => {
	// on mounted lets use the unique name
	localDefaultName.value = getUniqueName(localDefaultName.value, props.otherNames)
	nextTick(() => focusInput())
})
</script>

<style scoped>
.new-node-dialog__form {
	/* Ensure the dialog does not jump when there is a validity error */
	min-height: calc(3 * var(--default-clickable-area));
}
</style>
