import { create } from 'zustand'
import type { FilterState, PaginationState, Article } from '../types'

interface AppState {
  filters: FilterState
  pagination: PaginationState
  articles: Article[]
  totalCount: number
  lastSeen: {
    forward: { date: string; rowid: number }
    backward: { date: string; rowid: number }
  } | null
  currentSlide: number
  drawerOpen: boolean
  collectionTaskId: string | null
  downloadTaskId: string | null
  isCollecting: boolean
  isDownloading: boolean

  setFilters: (filters: Partial<FilterState>) => void
  setPagination: (pagination: Partial<PaginationState>) => void
  setArticles: (articles: Article[]) => void
  setTotalCount: (count: number) => void
  setLastSeen: (
    lastSeen: {
      forward: { date: string; rowid: number }
      backward: { date: string; rowid: number }
    } | null
  ) => void
  setCurrentSlide: (slide: number) => void
  toggleDrawer: () => void
  setCollectionTaskId: (taskId: string | null) => void
  setDownloadTaskId: (taskId: string | null) => void
  setIsCollecting: (isCollecting: boolean) => void
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
  drawerOpen: true,
  collectionTaskId: null,
  downloadTaskId: null,
  isCollecting: false,
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

  toggleDrawer: () => set((state) => ({ drawerOpen: !state.drawerOpen })),

  setCollectionTaskId: (taskId) => set({ collectionTaskId: taskId }),

  setDownloadTaskId: (taskId) => set({ downloadTaskId: taskId }),

  setIsCollecting: (isCollecting) => set({ isCollecting }),

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
