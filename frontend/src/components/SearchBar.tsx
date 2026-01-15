import { useState, useCallback } from 'react'
import { Box, TextInput } from '@mantine/core'
import { IconSearch } from '@tabler/icons-react'
import { useStore } from '../store'

function SearchBar() {
  const { filters, setFilters, resetPagination } = useStore()
  const [searchQuery, setSearchQuery] = useState(filters.query || '')

  const handleSearch = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (e.key === 'Enter') {
        setFilters({ query: searchQuery || null })
        resetPagination()
      }
    },
    [searchQuery, setFilters, resetPagination]
  )

  return (
    <Box w="60%" mx="auto" my="lg">
      <TextInput
        placeholder="Search articles by text..."
        value={searchQuery}
        onChange={(e) => setSearchQuery(e.target.value)}
        onKeyDown={handleSearch}
        leftSection={<IconSearch size={20} />}
        size="md"
        radius="md"
        styles={{
          input: {
            backgroundColor: 'white',
            border: '1px solid var(--mantine-color-gray-3)',
            '&:focus': {
              borderColor: 'var(--mantine-color-inky-red-4)',
            },
          },
        }}
      />
    </Box>
  )
}

export default SearchBar
