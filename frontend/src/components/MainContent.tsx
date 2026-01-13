import { useEffect } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { notifications } from '@mantine/notifications'
import {
  Box,
  Center,
  Text,
  Loader,
  ActionIcon,
  Paper,
  Stack,
  Container,
} from '@mantine/core'
import { IconDownload } from '@tabler/icons-react'
import { useStore } from '../store'
import { articlesApi, tasksApi } from '../api'
import Carousel from './Carousel'
import StatsChart from './StatsChart'

const SLIDES_PER_PAGE = 3
const MAX_PAGES = 10

function MainContent() {
  const {
    filters,
    pagination,
    setArticles,
    setTotalCount,
    setLastSeen,
    downloadTaskId,
    setDownloadTaskId,
    isDownloading,
    setIsDownloading,
  } = useStore()

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['articles', filters, pagination],
    queryFn: () =>
      articlesApi.getArticles(filters, pagination, SLIDES_PER_PAGE * MAX_PAGES),
    enabled: !!filters.dateStart && !!filters.dateEnd,
  })

  const { data: groupByData } = useQuery({
    queryKey: ['groupBy', filters, filters.groupBy],
    queryFn: () => articlesApi.getGroupBy(filters, filters.groupBy),
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

  const downloadMutation = useMutation({
    mutationFn: () => tasksApi.startDownload(filters, filters.descOrder),
    onSuccess: (data) => {
      setDownloadTaskId(data.task_id)
      setIsDownloading(true)
      notifications.show({
        title: 'Success',
        message: 'Download started!',
        color: 'green',
      })
    },
    onError: () => {
      notifications.show({
        title: 'Error',
        message: 'Failed to start download',
        color: 'red',
      })
    },
  })

  useEffect(() => {
    if (!isDownloading || !downloadTaskId) return

    const interval = setInterval(async () => {
      try {
        const status = await tasksApi.getDownloadStatus(downloadTaskId)
        if (status.status === 'SUCCESS') {
          setIsDownloading(false)
          setDownloadTaskId(null)
          window.location.href = '/api/v1/tasks/download/file'
        } else if (['FAILURE', 'REVOKED'].includes(status.status)) {
          setIsDownloading(false)
          setDownloadTaskId(null)
          notifications.show({
            title: 'Error',
            message: 'Download failed',
            color: 'red',
          })
        }
      } catch {
        // Ignore polling errors
      }
    }, 2000)

    return () => clearInterval(interval)
  }, [isDownloading, downloadTaskId, setIsDownloading, setDownloadTaskId])

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
    <Box maw={1200} mx="auto">
      <Box pos="relative" mb="md">
        <ActionIcon
          pos="absolute"
          top={0}
          right={0}
          variant="subtle"
          color="red"
          onClick={() => downloadMutation.mutate()}
          disabled={isDownloading}
          title="Download data"
          size="lg"
        >
          <IconDownload size={20} />
        </ActionIcon>

        {groupByData && (
          <StatsChart data={groupByData.data} reversed={!filters.descOrder} />
        )}
      </Box>

      <Carousel
        articles={data.articles}
        slidesPerPage={SLIDES_PER_PAGE}
        maxPages={MAX_PAGES}
      />
    </Box>
  )
}

export default MainContent
