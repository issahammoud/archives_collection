import { useState, useCallback } from 'react'
import { Box, TextInput } from '@mantine/core'
import { useMediaQuery } from '@mantine/hooks'
import { IconSearch } from '@tabler/icons-react'
import { useStore } from '../store'

function SearchBar() {
  const { filters, setFilters, resetPagination } = useStore()
  const [searchQuery, setSearchQuery] = useState(filters.query || '')
  const isMobile = useMediaQuery('(max-width: 768px)')

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
    <Box w={isMobile ? '100%' : '60%'} mx="auto" my={isMobile ? 'md' : 'lg'} p={isMobile ? '0' : 'lg'}>
      <TextInput
        placeholder={isMobile ? 'Guerre en Ukraine, Gaza Genocide, etc.' : 'Search for articles, Gaza War, Beyrouth Port Explosion etc.'}
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
