export interface Article {
  rowid: number
  date: string
  archive: string
  image: string | null
  title: string | null
  content: string | null
  tag: string | null
  link: string
}

export interface ArticlesResponse {
  articles: Article[]
  total_count: number
  last_seen: {
    forward: { date: string; rowid: number }
    backward: { date: string; rowid: number }
  } | null
}

export interface GroupByData {
  date: string
  count: number
}

export interface GroupByResponse {
  data: GroupByData[]
}

export interface ArchiveCountData {
  archive: string
  count: number
}

export interface ArchiveCountsResponse {
  data: ArchiveCountData[]
}

export interface TagsResponse {
  tags: string[]
}

export interface DateRangeResponse {
  min_date: string | null
  max_date: string | null
}

export interface TaskResponse {
  task_id: string
  status: string
  task_name: string
}

export interface TaskStatusResponse {
  task_id: string
  status: string
  result: unknown
}

export interface FilterState {
  archives: string[]
  tag: string | null
  dateStart: string | null
  dateEnd: string | null
  query: string | null
  hasImage: boolean
  descOrder: boolean
  groupBy: 'day' | 'month' | 'year'
}

export interface PaginationState {
  lastSeenDate: string | null
  lastSeenRowid: number | null
  direction: 'forward' | 'backward'
}
