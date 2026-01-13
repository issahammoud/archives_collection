import { ActionIcon } from '@mantine/core'
import { IconChevronLeft, IconChevronRight } from '@tabler/icons-react'
import { useStore } from '../store'

function SidebarToggle() {
  const { drawerOpen, toggleDrawer } = useStore()

  return (
    <ActionIcon
      onClick={toggleDrawer}
      variant="subtle"
      radius="xl"
      size={30}
      style={{
        position: 'fixed',
        top: '50%',
        left: drawerOpen ? 'calc(15% - 15px)' : 5,
        zIndex: 1000,
        transform: 'translateY(-50%)',
        backgroundColor: 'white',
        boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
        border: '1px solid #eee',
        transition: 'left 300ms ease',
      }}
    >
      {drawerOpen ? (
        <IconChevronLeft size={20} color="#ff5757" />
      ) : (
        <IconChevronRight size={20} color="#ff5757" />
      )}
    </ActionIcon>
  )
}

export default SidebarToggle
