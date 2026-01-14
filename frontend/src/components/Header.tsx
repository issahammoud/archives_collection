import { Container, Group, Stack, Text } from '@mantine/core'
import { IconNews } from '@tabler/icons-react'

function Header() {
  return (
    <Container size="xl" h="100%" style={{ display: 'flex', alignItems: 'center' }}>
      <Group>
        <IconNews size={50} color="var(--mantine-color-inky-red-4)" />
        <Stack gap={0}>
          <Text size="lg" fw={600} c="inky-navy.6">
            News DB
          </Text>
          <Text size="xs" c="inky-navy.6">
            By InkyLab AI
          </Text>
        </Stack>
      </Group>
    </Container>
  )
}

export default Header
