package org.icpc.tools.cds.service;

import java.io.IOException;
import java.io.Writer;

import jakarta.websocket.Session;

public class WebSocketLineWriter extends Writer {
    private final Session session;
    private final StringBuilder buffer = new StringBuilder(32 * 1024);

    public WebSocketLineWriter(Session session) {
        this.session = session;
    }

    @Override
    public synchronized void write(char[] cbuf, int off, int len) throws IOException {
        if (!session.isOpen())
            return;

        int start = off;
        int end = off + len;
        for (int i = off; i < end; i++) {
            if (cbuf[i] == '\n') {
                if (i > start)
                    buffer.append(cbuf, start, i - start);

                String msg = (buffer.length() == 0) ? "\n" : buffer.toString() + "\n";
                session.getBasicRemote().sendText(msg);
                buffer.setLength(0);
                start = i + 1;
            }
        }

        if (start < end)
            buffer.append(cbuf, start, end - start);
    }

    @Override
    public synchronized void flush() throws IOException {
        if (!session.isOpen())
            return;
        if (buffer.length() > 0) {
            session.getBasicRemote().sendText(buffer.toString());
            buffer.setLength(0);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        flush();
    }
}
