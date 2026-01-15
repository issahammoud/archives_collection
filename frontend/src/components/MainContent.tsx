import { useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Box,
  Center,
  Text,
  Loader,
  Paper,
  Stack,
} from '@mantine/core'
import { useStore } from '../store'
import { articlesApi } from '../api'
import Carousel from './Carousel'

const SLIDES_PER_PAGE = 2
const MAX_PAGES = 10

function MainContent() {
  const {
    filters,
    pagination,
    setArticles,
    setTotalCount,
    setLastSeen,
  } = useStore()

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['articles', filters, pagination],
    queryFn: () =>
      articlesApi.getArticles(filters, pagination, SLIDES_PER_PAGE * MAX_PAGES),
    enabled: !!filters.dateStart && !!filters.dateEnd,
  })

  const { setCurrentSlide } = useStore()

  useEffect(() => {
    if (data) {
      setArticles(data.articles)
      setTotalCount(data.total_count)
      setLastSeen(data.last_seen)
      // Position carousel based on navigation direction
      if (pagination.lastSeenDate) {
        if (pagination.direction === 'forward') {
          setCurrentSlide(0) // Start of new batch
        } else {
          setCurrentSlide(MAX_PAGES - 1) // End of previous batch
        }
      }
    }
  }, [data, setArticles, setTotalCount, setLastSeen, pagination.lastSeenDate, pagination.direction, setCurrentSlide])

  useEffect(() => {
    const interval = setInterval(() => {
      refetch()
    }, 5000)

    return () => clearInterval(interval)
  }, [refetch])

  if (!filters.dateStart || !filters.dateEnd) {
    return (
      <Center h={400}>
        <Loader color="red" size="lg" />
      </Center>
    )
  }

  if (isLoading) {
    return (
      <Center h={400}>
        <Loader color="red" size="lg" />
      </Center>
    )
  }

  if (!data?.articles || data.articles.length <= SLIDES_PER_PAGE) {
    return (
      <Center h={400}>
        <Paper p="xl" withBorder radius="md" bg="red.0">
          <Stack align="center" gap="xs">
            <Text size="lg" fw={500} c="red.8">
              Sorry
            </Text>
            <Text c="red.6">
              We didn't find any data with your current filters.
            </Text>
          </Stack>
        </Paper>
      </Center>
    )
  }

  return (
    <Box
      p="lg"
      h="100%"
      style={{ borderRadius: 'var(--mantine-radius-md)' }}
    >
      <Carousel
        articles={data.articles}
        slidesPerPage={SLIDES_PER_PAGE}
        maxPages={MAX_PAGES}
      />
    </Box>
  )
}

export default MainContent
