import { useState, useCallback, useEffect } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { notifications } from '@mantine/notifications'
import {
  Stack,
  Group,
  Badge,
  ActionIcon,
  Select,
  TextInput,
  MultiSelect,
  Text,
  Popover,
  Radio,
  Tooltip,
} from '@mantine/core'
import { DatePickerInput } from '@mantine/dates'
import {
  IconArrowsSort,
  IconPhotoOff,
  IconChartBar,
  IconSearch,
  IconBook,
  IconCalendar,
  IconTag,
  IconDownload,
} from '@tabler/icons-react'
import { useStore } from '../store'
import { articlesApi, tasksApi } from '../api'
import dayjs from 'dayjs'

function Sidebar() {
  const {
    filters,
    setFilters,
    totalCount,
    downloadTaskId,
    setDownloadTaskId,
    isDownloading,
    setIsDownloading,
    resetPagination,
  } = useStore()

  const [searchQuery, setSearchQuery] = useState('')

  const { data: tagsData } = useQuery({
    queryKey: ['tags'],
    queryFn: articlesApi.getTags,
  })

  const { data: archivesData } = useQuery({
    queryKey: ['archives'],
    queryFn: articlesApi.getArchives,
  })

  const { data: dateRange } = useQuery({
    queryKey: ['dateRange'],
    queryFn: articlesApi.getDateRange,
  })

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

  const handleSearch = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (e.key === 'Enter') {
        setFilters({ query: searchQuery || null })
        resetPagination()
      }
    },
    [searchQuery, setFilters, resetPagination]
  )

  const formatCount = (count: number): string => {
    if (count >= 1000000) return `${(count / 1000000).toFixed(1)}M`
    if (count >= 1000) return `${(count / 1000).toFixed(1)}K`
    return String(count)
  }

  return (
    <Stack gap="xl">
      <Badge
        size="xl"
        variant="filled"
        color="gray.0"
        radius="sm"
        fullWidth
        c="inky-navy.6"
      >
        {`${formatCount(totalCount)} articles`}
      </Badge>

      {/* Switches */}
      <Group justify="space-between" gap="sm">
        <Tooltip label="Flip order" withArrow position="top">
          <ActionIcon
            variant="light"
            color="inky-red.4"
            onClick={() => {
              setFilters({ descOrder: !filters.descOrder })
              resetPagination()
            }}
            size="lg"
          >
            <IconArrowsSort size={20} />
          </ActionIcon>
        </Tooltip>
        <Tooltip label="Filter images" withArrow position="top">
          <ActionIcon
            variant={filters.hasImage ? 'filled' : 'light'}
            color="inky-red.4"
            onClick={() => {
              setFilters({ hasImage: !filters.hasImage })
              resetPagination()
            }}
            size="lg"
          >
            <IconPhotoOff size={20} />
          </ActionIcon>
        </Tooltip>
        <Popover position="bottom" withArrow shadow="md">
          <Popover.Target>
            <Tooltip label="Group by" withArrow position="top">
              <ActionIcon variant="light" color="inky-red.4" size="lg">
                <IconChartBar size={20} />
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
                <Radio value="day" label="day" />
                <Radio value="month" label="month" />
                <Radio value="year" label="year" />
              </Stack>
            </Radio.Group>
          </Popover.Dropdown>
        </Popover>
        <Tooltip label="Download data" withArrow position="top">
          <ActionIcon
            variant="light"
            color="inky-red.4"
            onClick={() => downloadMutation.mutate()}
            disabled={isDownloading}
            size="lg"
          >
            <IconDownload size={20} />
          </ActionIcon>
        </Tooltip>
      </Group>

      {/* Date picker */}
      <DatePickerInput
        type="range"
        label={<Text c="inky-navy.6">Date Range</Text>}
        placeholder="Select dates"
        value={[
          filters.dateStart ? dayjs(filters.dateStart).toDate() : null,
          filters.dateEnd ? dayjs(filters.dateEnd).toDate() : null,
        ]}
        onChange={(value) => {
          setFilters({
            dateStart: value[0] ? dayjs(value[0]).format('YYYY-MM-DD') : null,
            dateEnd: value[1] ? dayjs(value[1]).format('YYYY-MM-DD') : null,
          })
          resetPagination()
        }}
        minDate={dateRange?.min_date ? dayjs(dateRange.min_date).toDate() : undefined}
        maxDate={dateRange?.max_date ? dayjs(dateRange.max_date).toDate() : undefined}
        valueFormat="DD/MM/YYYY"
        leftSection={<IconCalendar size={16} />}
        variant="default"
        radius="md"
        clearable={false}
      />

      {/* Topic select */}
      <Select
        label={<Text c="inky-navy.6">Topic</Text>}
        placeholder="Search by topic"
        value={filters.tag}
        onChange={(value) => {
          setFilters({ tag: value })
          resetPagination()
        }}
        data={tagsData?.tags.map((tag) => ({ value: tag, label: tag })) || []}
        clearable
        searchable
        leftSection={<IconTag size={16} />}
        variant="default"
        radius="md"
        comboboxProps={{ transitionProps: { transition: 'pop', duration: 200 } }}
      />

      {/* Text search */}
      <TextInput
        label={<Text c="inky-navy.6">Text</Text>}
        placeholder="Search by text"
        value={searchQuery}
        onChange={(e) => setSearchQuery(e.target.value)}
        onKeyDown={handleSearch}
        leftSection={<IconSearch size={16} />}
        variant="default"
        radius="md"
      />

      {/* Archives multi-select */}
      <MultiSelect
        label={<Text c="inky-navy.6">Archive</Text>}
        placeholder="Search by archive"
        value={filters.archives}
        onChange={(value) => {
          setFilters({ archives: value })
          resetPagination()
        }}
        data={archivesData?.archives || []}
        clearable
        searchable
        hidePickedOptions
        leftSection={<IconBook size={16} />}
        variant="default"
        radius="md"
        comboboxProps={{ transitionProps: { transition: 'pop', duration: 200 } }}
      />
    </Stack>
  )
}

export default Sidebar
