import { create } from 'zustand'
import type { FilterState, PaginationState, Article } from '../types'

interface LastSeenCursor {
  date: string
  rowid: number
}

interface LastSeenState {
  forward: LastSeenCursor | null
  backward: LastSeenCursor | null
}

interface AppState {
  filters: FilterState
  pagination: PaginationState
  articles: Article[]
  totalCount: number
  lastSeen: LastSeenState | null
  currentSlide: number
  downloadTaskId: string | null
  isDownloading: boolean

  setFilters: (filters: Partial<FilterState>) => void
  setPagination: (pagination: Partial<PaginationState>) => void
  setArticles: (articles: Article[]) => void
  setTotalCount: (count: number) => void
  setLastSeen: (lastSeen: LastSeenState | null) => void
  setCurrentSlide: (slide: number) => void
  setDownloadTaskId: (taskId: string | null) => void
  setIsDownloading: (isDownloading: boolean) => void
  resetPagination: () => void
}

export const useStore = create<AppState>((set) => ({
  filters: {
    archives: [],
    tag: null,
    dateStart: null,
    dateEnd: null,
    query: null,
    hasImage: false,
    descOrder: true,
    groupBy: 'month',
  },
  pagination: {
    lastSeenDate: null,
    lastSeenRowid: null,
    direction: 'forward',
  },
  articles: [],
  totalCount: 0,
  lastSeen: null,
  currentSlide: 0,
  downloadTaskId: null,
  isDownloading: false,

  setFilters: (newFilters) =>
    set((state) => ({
      filters: { ...state.filters, ...newFilters },
    })),

  setPagination: (newPagination) =>
    set((state) => ({
      pagination: { ...state.pagination, ...newPagination },
    })),

  setArticles: (articles) => set({ articles }),

  setTotalCount: (count) => set({ totalCount: count }),

  setLastSeen: (lastSeen) => set({ lastSeen }),

  setCurrentSlide: (slide) => set({ currentSlide: slide }),

  setDownloadTaskId: (taskId) => set({ downloadTaskId: taskId }),

  setIsDownloading: (isDownloading) => set({ isDownloading }),

  resetPagination: () =>
    set({
      pagination: {
        lastSeenDate: null,
        lastSeenRowid: null,
        direction: 'forward',
      },
      currentSlide: 0,
    }),
}))
