import { useEffect, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Box,
  Center,
  Text,
  Paper,
  Stack,
} from '@mantine/core'
import { useStore } from '../store'
import { articlesApi } from '../api'
import Carousel from './Carousel'
import { CarouselSkeleton } from './Skeletons'

const CARDS_PER_PAGE = 9
const MAX_PAGES = 5

function MainContent() {
  const {
    filters,
    pagination,
    articles: storedArticles,
    setArticles,
    setTotalCount,
    setLastSeen,
    resetPagination,
  } = useStore()

  // Track if we're navigating (vs initial load or filter change)
  const isNavigating = useRef(false)

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['articles', filters, pagination],
    queryFn: () =>
      articlesApi.getArticles(filters, pagination, CARDS_PER_PAGE * MAX_PAGES),
    enabled: !!filters.dateStart && !!filters.dateEnd,
  })

  // Track when pagination changes (navigation)
  useEffect(() => {
    if (pagination.lastSeenDate) {
      isNavigating.current = true
    }
  }, [pagination.lastSeenDate, pagination.lastSeenRowid])

  // Store ref to track last processed data to avoid re-processing
  const lastProcessedData = useRef<typeof data | null>(null)

  useEffect(() => {
    if (!data || data === lastProcessedData.current) {
      return
    }

    // If navigating and got empty results, stay on current data
    if (isNavigating.current && data.articles.length === 0) {
      // Disable navigation in the attempted direction using current store value
      const currentLastSeen = useStore.getState().lastSeen
      if (currentLastSeen) {
        const updatedLastSeen = { ...currentLastSeen }
        if (pagination.direction === 'forward') {
          updatedLastSeen.forward = null
        } else {
          updatedLastSeen.backward = null
        }
        setLastSeen(updatedLastSeen)
      }
      // Reset pagination to go back to stored articles
      resetPagination()
      isNavigating.current = false
      lastProcessedData.current = data
      return
    }

    setArticles(data.articles)
    setTotalCount(data.total_count)

    // If we got fewer articles than requested, we've reached the end
    // Disable forward navigation in that case
    const requestedLimit = CARDS_PER_PAGE * MAX_PAGES
    if (data.articles.length < requestedLimit && data.last_seen) {
      setLastSeen({
        ...data.last_seen,
        forward: null, // No more data forward
      })
    } else {
      setLastSeen(data.last_seen)
    }

    isNavigating.current = false
    lastProcessedData.current = data
  }, [data, pagination.direction, pagination.lastSeenDate, setArticles, setTotalCount, setLastSeen, resetPagination])

  useEffect(() => {
    const interval = setInterval(() => {
      refetch()
    }, 5000)

    return () => clearInterval(interval)
  }, [refetch])

  // Use stored articles if current data is empty (boundary case)
  const displayArticles = data?.articles?.length ? data.articles : storedArticles

  // Show error state
  if (error) {
    return (
      <Center h={400}>
        <Paper p="xl" withBorder radius="md" bg="red.0">
          <Stack align="center" gap="xs">
            <Text size="lg" fw={500} c="red.8">
              Error
            </Text>
            <Text c="red.6">
              Something went wrong. Please try again.
            </Text>
          </Stack>
        </Paper>
      </Center>
    )
  }

  // Show skeleton only on initial load (when no data exists yet)
  if (isLoading && (!storedArticles || storedArticles.length === 0)) {
    return (
      <Box
        p="lg"
        h="100%"
        style={{ borderRadius: 'var(--mantine-radius-md)' }}
      >
        <CarouselSkeleton />
      </Box>
    )
  }

  if (!displayArticles || displayArticles.length === 0) {
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
        articles={displayArticles}
        slidesPerPage={CARDS_PER_PAGE}
        maxPages={MAX_PAGES}
      />
    </Box>
  )
}

export default MainContent
