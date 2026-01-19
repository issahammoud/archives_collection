import axios from 'axios'
import type {
  ArticlesResponse,
  GroupByResponse,
  ArchiveCountsResponse,
  TagsResponse,
  DateRangeResponse,
  TaskResponse,
  TaskStatusResponse,
  FilterState,
  PaginationState,
} from '../types'

const api = axios.create({
  baseURL: '/api/v1',
})

export const articlesApi = {
  getArticles: async (
    filters: FilterState,
    pagination: PaginationState,
    limit: number = 30
  ): Promise<ArticlesResponse> => {
    const params = new URLSearchParams()

    if (filters.archives.length > 0) {
      filters.archives.forEach((a) => params.append('archives', a))
    }
    if (filters.tag) params.set('tag', filters.tag)
    if (filters.dateStart) params.set('date_start', filters.dateStart)
    if (filters.dateEnd) params.set('date_end', filters.dateEnd)
    if (filters.query) params.set('query', filters.query)
    if (filters.hasImage) params.set('has_image', 'true')
    params.set('desc_order', String(filters.descOrder))
    params.set('limit', String(limit))
    params.set('direction', pagination.direction)

    if (pagination.lastSeenDate && pagination.lastSeenRowid) {
      params.set('last_seen_date', pagination.lastSeenDate)
      params.set('last_seen_rowid', String(pagination.lastSeenRowid))
    }

    const { data } = await api.get<ArticlesResponse>(`/articles?${params}`)
    return data
  },

  getCount: async (filters: FilterState): Promise<{ count: number }> => {
    const params = new URLSearchParams()

    if (filters.archives.length > 0) {
      filters.archives.forEach((a) => params.append('archives', a))
    }
    if (filters.tag) params.set('tag', filters.tag)
    if (filters.dateStart) params.set('date_start', filters.dateStart)
    if (filters.dateEnd) params.set('date_end', filters.dateEnd)
    if (filters.query) params.set('query', filters.query)
    if (filters.hasImage) params.set('has_image', 'true')

    const { data } = await api.get<{ count: number }>(`/articles/count?${params}`)
    return data
  },

  getGroupBy: async (
    filters: FilterState,
    groupBy: 'day' | 'month' | 'year'
  ): Promise<GroupByResponse> => {
    const params = new URLSearchParams()

    if (filters.archives.length > 0) {
      filters.archives.forEach((a) => params.append('archives', a))
    }
    if (filters.tag) params.set('tag', filters.tag)
    if (filters.dateStart) params.set('date_start', filters.dateStart)
    if (filters.dateEnd) params.set('date_end', filters.dateEnd)
    if (filters.query) params.set('query', filters.query)
    if (filters.hasImage) params.set('has_image', 'true')
    params.set('group_by', groupBy)

    const { data } = await api.get<GroupByResponse>(`/articles/group-by?${params}`)
    return data
  },

  getDateRange: async (): Promise<DateRangeResponse> => {
    const { data } = await api.get<DateRangeResponse>('/articles/date-range')
    return data
  },

  getArchives: async (): Promise<{ archives: string[] }> => {
    const { data } = await api.get<{ archives: string[] }>('/articles/archives')
    return data
  },

  getArchiveCounts: async (filters: FilterState): Promise<ArchiveCountsResponse> => {
    const params = new URLSearchParams()

    if (filters.archives.length > 0) {
      filters.archives.forEach((a) => params.append('archives', a))
    }
    if (filters.tag) params.set('tag', filters.tag)
    if (filters.dateStart) params.set('date_start', filters.dateStart)
    if (filters.dateEnd) params.set('date_end', filters.dateEnd)
    if (filters.query) params.set('query', filters.query)
    if (filters.hasImage) params.set('has_image', 'true')

    const { data } = await api.get<ArchiveCountsResponse>(`/articles/archive-counts?${params}`)
    return data
  },
}

export const tasksApi = {
  startCollection: async (
    archives: string[] | null,
    beginDate: string,
    endDate: string
  ): Promise<TaskResponse> => {
    const { data } = await api.post<TaskResponse>('/tasks/collection/start', {
      archives,
      begin_date: beginDate,
      end_date: endDate,
    })
    return data
  },

  stopCollection: async (taskId: string): Promise<{ status: string }> => {
    const { data } = await api.post<{ status: string }>(
      `/tasks/collection/stop?task_id=${taskId}`
    )
    return data
  },

  getCollectionStatus: async (taskId: string): Promise<TaskStatusResponse> => {
    const { data } = await api.get<TaskStatusResponse>(
      `/tasks/collection/status/${taskId}`
    )
    return data
  },

  startDownload: async (
    filters: FilterState,
    order: boolean
  ): Promise<TaskResponse> => {
    const params = new URLSearchParams()

    if (filters.archives.length > 0) {
      filters.archives.forEach((a) => params.append('archives', a))
    }
    if (filters.tag) params.set('tag', filters.tag)
    if (filters.dateStart) params.set('date_start', filters.dateStart)
    if (filters.dateEnd) params.set('date_end', filters.dateEnd)
    if (filters.query) params.set('query', filters.query)
    if (filters.hasImage) params.set('has_image', 'true')
    params.set('order', String(order))

    const { data } = await api.post<TaskResponse>(`/tasks/download/start?${params}`)
    return data
  },

  getDownloadStatus: async (taskId: string): Promise<TaskStatusResponse> => {
    const { data } = await api.get<TaskStatusResponse>(
      `/tasks/download/status/${taskId}`
    )
    return data
  },
}

export const imagesApi = {
  getImageUrl: (path: string): string => {
    if (!path) return ''
    const cleanPath = path.startsWith('/images/')
      ? path.replace('/images/', '')
      : path
    return `/api/v1/images/thumbnail/${cleanPath}`
  },

  getRawImageUrl: (path: string): string => {
    if (!path) return ''
    const cleanPath = path.startsWith('/images/')
      ? path.replace('/images/', '')
      : path
    return `/api/v1/images/raw/${cleanPath}`
  },
}
