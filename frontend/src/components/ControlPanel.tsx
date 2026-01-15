import { useEffect } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { notifications } from '@mantine/notifications'
import {
  Stack,
  ActionIcon,
  Popover,
  Radio,
  Tooltip,
  Box,
} from '@mantine/core'
import {
  IconArrowsSort,
  IconPhotoOff,
  IconChartBar,
  IconDownload,
} from '@tabler/icons-react'
import { useStore } from '../store'
import { tasksApi } from '../api'

function ControlPanel() {
  const {
    filters,
    setFilters,
    downloadTaskId,
    setDownloadTaskId,
    isDownloading,
    setIsDownloading,
    resetPagination,
  } = useStore()

  const downloadMutation = useMutation({
    mutationFn: () => tasksApi.startDownload(filters, filters.descOrder),
    onSuccess: (data) => {
      setDownloadTaskId(data.task_id)
      setIsDownloading(true)
      notifications.show({
        title: 'Success',
        message: 'Download started!',
        color: 'inky-navy',
      })
    },
    onError: () => {
      notifications.show({
        title: 'Error',
        message: 'Failed to start download',
        color: 'inky-red.4',
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
            color: 'inky-red.4',
          })
        }
      } catch {
        // Ignore polling errors
      }
    }, 2000)

    return () => clearInterval(interval)
  }, [isDownloading, downloadTaskId, setIsDownloading, setDownloadTaskId])

  return (
    <Box
      style={{
        position: 'sticky',
        top: 'var(--mantine-spacing-md)',
        height: 'fit-content',
      }}
    >
      <Stack justify="space-between">
        <Tooltip label="Flip order" withArrow position="left">
          <ActionIcon
            variant="light"
            color="inky-red.4"
            onClick={() => {
              setFilters({ descOrder: !filters.descOrder })
              resetPagination()
            }}
            size="xl"
            radius="md"
          >
            <IconArrowsSort size={22} />
          </ActionIcon>
        </Tooltip>

        <Tooltip label="Filter images only" withArrow position="left">
          <ActionIcon
            variant={filters.hasImage ? 'filled' : 'light'}
            color="inky-red.4"
            onClick={() => {
              setFilters({ hasImage: !filters.hasImage })
              resetPagination()
            }}
            size="xl"
            radius="md"
          >
            <IconPhotoOff size={22} />
          </ActionIcon>
        </Tooltip>

        <Popover position="left" withArrow shadow="md">
          <Popover.Target>
            <Tooltip label="Group by" withArrow position="left">
              <ActionIcon variant="light" color="inky-red.4" size="xl" radius="md">
                <IconChartBar size={22} />
              </ActionIcon>
            </Tooltip>
          </Popover.Target>
          <Popover.Dropdown>
            <Radio.Group
              value={filters.groupBy}
              onChange={(value) => setFilters({ groupBy: value as 'day' | 'month' | 'year' })}
              size="xs"
            >
              <Stack gap="xs">
                <Radio value="day" label="Day" />
                <Radio value="month" label="Month" />
                <Radio value="year" label="Year" />
              </Stack>
            </Radio.Group>
          </Popover.Dropdown>
        </Popover>

        <Tooltip label="Download data" withArrow position="left">
          <ActionIcon
            variant="light"
            color="inky-red.4"
            onClick={() => downloadMutation.mutate()}
            disabled={isDownloading}
            size="xl"
            radius="md"
            loading={isDownloading}
          >
            <IconDownload size={22} />
          </ActionIcon>
        </Tooltip>
      </Stack>
    </Box>
  )
}

export default ControlPanel
