use glommio::io::DmaStreamWriter;

struct Writer{
    file: DmaStreamWriter,
}