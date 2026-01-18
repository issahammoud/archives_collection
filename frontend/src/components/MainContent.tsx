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

  const isNavigating = useRef(false)
  const lastProcessedData = useRef<string | null>(null) // Store as stringified key to detect real changes

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['articles', filters, pagination],
    queryFn: () =>
      articlesApi.getArticles(filters, pagination, CARDS_PER_PAGE * MAX_PAGES),
    enabled: !!filters.dateStart && !!filters.dateEnd,
  })

  // Track when navigation occurs
  useEffect(() => {
    if (pagination.lastSeenDate || pagination.lastSeenRowid) {
      isNavigating.current = true
    }
  }, [pagination.lastSeenDate, pagination.lastSeenRowid])

  // Main synchronization effect
  useEffect(() => {
    // 1. Guard: Only proceed if we have data and it's actually "new" data
    // We use a JSON string check because React Query might return a new object reference 
    // for the same data content, which triggers the effect loop.
    const dataSignature = JSON.stringify(data);
    if (!data || dataSignature === lastProcessedData.current) {
      return;
    }

    // 2. Handle the "No Results Found" case while navigating
    if (isNavigating.current && data.articles.length === 0) {
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
      
      lastProcessedData.current = dataSignature; // Mark as processed BEFORE state update
      isNavigating.current = false;
      resetPagination(); 
      return;
    }

    // 3. Successful Data Update
    setArticles(data.articles)
    setTotalCount(data.total_count)

    // 4. Update Pagination Boundaries
    const requestedLimit = CARDS_PER_PAGE * MAX_PAGES
    if (data.articles.length < requestedLimit && data.last_seen) {
      setLastSeen({
        ...data.last_seen,
        forward: null,
      })
    } else {
      setLastSeen(data.last_seen)
    }

    // Finalize processing
    lastProcessedData.current = dataSignature;
    isNavigating.current = false;

    // We keep dependencies minimal to avoid re-triggering on local pagination changes 
    // unless the underlying DATA has actually changed via the API.
  }, [data, setArticles, setTotalCount, setLastSeen, resetPagination, pagination.direction])

  // Periodic Refetch
  useEffect(() => {
    const interval = setInterval(() => {
      refetch()
    }, 5000)
    return () => clearInterval(interval)
  }, [refetch])

  const displayArticles = data?.articles?.length ? data.articles : storedArticles

  if (error) {
    return (
      <Center h={400}>
        <Paper p="xl" withBorder radius="md" bg="red.0">
          <Stack align="center" gap="xs">
            <Text size="lg" fw={500} c="red.8">Error</Text>
            <Text c="red.6">Something went wrong. Please try again.</Text>
          </Stack>
        </Paper>
      </Center>
    )
  }

  if (isLoading && (!storedArticles || storedArticles.length === 0)) {
    return (
      <Box p="lg" h="100%" style={{ borderRadius: 'var(--mantine-radius-md)' }}>
        <CarouselSkeleton />
      </Box>
    )
  }

  if (!displayArticles || displayArticles.length === 0) {
    return (
      <Center h={400}>
        <Paper p="xl" withBorder radius="md" bg="red.0">
          <Stack align="center" gap="xs">
            <Text size="lg" fw={500} c="red.8">Sorry</Text>
            <Text c="red.6">We didn't find any data with your current filters.</Text>
          </Stack>
        </Paper>
      </Center>
    )
  }

  return (
    <Box p="lg" h="100%" style={{ borderRadius: 'var(--mantine-radius-md)' }}>
      <Carousel
        articles={displayArticles}
        slidesPerPage={CARDS_PER_PAGE}
        maxPages={MAX_PAGES}
      />
    </Box>
  )
}

export default MainContent