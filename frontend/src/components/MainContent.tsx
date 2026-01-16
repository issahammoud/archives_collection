import { useEffect, useRef } from 'react'
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

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['articles', filters, pagination],
    queryFn: () =>
      articlesApi.getArticles(filters, pagination, CARDS_PER_PAGE * MAX_PAGES),
    enabled: !!filters.dateStart && !!filters.dateEnd,
  })

  const { setCurrentSlide } = useStore()

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

    // Position carousel based on navigation direction
    if (pagination.lastSeenDate) {
      if (pagination.direction === 'forward') {
        setCurrentSlide(0) // Start of new batch
      } else {
        setCurrentSlide(MAX_PAGES - 1) // End of previous batch
      }
    }
    isNavigating.current = false
    lastProcessedData.current = data
  }, [data, pagination.direction, pagination.lastSeenDate, setArticles, setTotalCount, setLastSeen, setCurrentSlide, resetPagination])

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

  // Use stored articles if current data is empty (boundary case)
  const displayArticles = data?.articles?.length ? data.articles : storedArticles

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
