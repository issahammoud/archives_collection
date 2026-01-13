import { Container, Image } from '@mantine/core'

function Header() {
  return (
    <Container size="xl" h="100%" py="md">
      <Image
        src="/logo.png"
        alt="Logo"
        h={80}
        w="auto"
        fit="contain"
        fallbackSrc=""
      />
    </Container>
  )
}

export default Header
