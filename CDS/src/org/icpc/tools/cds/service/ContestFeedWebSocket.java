package org.icpc.tools.cds.service;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.ByteBuffer;

import org.icpc.tools.cds.CDSConfig;
import org.icpc.tools.cds.ConfiguredContest;
import org.icpc.tools.cds.service.ContestFeedExecutor.Feed;
import org.icpc.tools.cds.service.ContestObjectQueue.ContestObjectDelta;
import org.icpc.tools.cds.presentations.WebSocketConfig;
import org.icpc.tools.contest.Trace;
import org.icpc.tools.contest.model.IContestListener;
import org.icpc.tools.contest.model.IContestObject;
import org.icpc.tools.contest.model.feed.NDJSONFeedWriter;
import org.icpc.tools.contest.model.internal.Contest;

import jakarta.websocket.CloseReason;
import jakarta.websocket.CloseReason.CloseCodes;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.PathParam;
import jakarta.websocket.server.ServerEndpoint;

@ServerEndpoint(value = "/api/contests/{contestId}/event-feed/ws", configurator = WebSocketConfig.class)
public class ContestFeedWebSocket {
    private Contest contest;
    private ConfiguredContest cc;
    private IContestListener listener;

    @OnOpen
    public void onOpen(Session session, @PathParam("contestId") String contestId, EndpointConfig config) {
        session.setMaxTextMessageBufferSize(500 * 1024);
        session.setMaxIdleTimeout(60000);
        session.getContainer().setAsyncSendTimeout(15000);

        cc = CDSConfig.getContest(contestId);
        if (cc == null) {
            Trace.trace(Trace.WARNING, "Contest not found: " + contestId);
            try {
                session.close(new CloseReason(CloseCodes.UNEXPECTED_CONDITION, "Contest " + contestId + " not found"));
            } catch (IOException e) {
                Trace.trace(Trace.ERROR, "Error closing websocket", e);
            }
            return;
        }

        try {
            contest = cc.getContestByRole(session);
            if (contest == null) {
                Trace.trace(Trace.WARNING, "Contest instance is null");
                session.close(new CloseReason(CloseCodes.UNEXPECTED_CONDITION, "Failed to get contest instance"));
                return;
            }
            if (contest.getInfo() == null) {
                Trace.trace(Trace.WARNING, "Contest not properly configured");
                session.close(new CloseReason(CloseCodes.UNEXPECTED_CONDITION, "Contest not properly configured"));
                return;
            }
        } catch (Exception e) {
            Trace.trace(Trace.ERROR, "Error getting contest instance", e);
            try {
                session.close(new CloseReason(CloseCodes.UNEXPECTED_CONDITION,
                        "Contest initialization error: " + e.getMessage()));
            } catch (IOException ex) {
                Trace.trace(Trace.ERROR, "Error closing websocket", ex);
            }
            return;
        }

        CompositeFilter filter = new CompositeFilter();

        String user = session.getUserPrincipal() != null ? session.getUserPrincipal().getName() : "anonymous";
        PrintWriter pw = new PrintWriter(new Writer() {
            private final StringBuilder buffer = new StringBuilder(32 * 1024); // 32KB buffer

            @Override
            public synchronized void write(char[] cbuf, int off, int len) throws IOException {
                if (!session.isOpen())
                    return;

                int start = off;
                int end = off + len;
                for (int i = off; i < end; i++) {
                    if (cbuf[i] == '\n') {
                        // append up to before newline
                        if (i > start)
                            buffer.append(cbuf, start, i - start);

                        // send one complete message per line (including newline for heartbeats)
                        String msg;
                        if (buffer.length() == 0) {
                            // heartbeat (empty line)
                            msg = "\n";
                        } else {
                            msg = buffer.toString() + "\n";
                        }
                        session.getBasicRemote().sendText(msg);
                        buffer.setLength(0);
                        start = i + 1; // next segment starts after newline
                    }
                }

                // append any remaining (no newline encountered)
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
        }, true);

        final NDJSONFeedWriter writer = new NDJSONFeedWriter(pw);
        final String prefix = NDJSONFeedWriter.getContestPrefix(contest);
        final ContestObjectQueue queue = new ContestObjectQueue(0);
        listener = (contest2, obj, delta) -> queue.add(obj, delta);
        cc.add(session);
        ContestFeedExecutor.getInstance().addFeedSource(new Feed() {
            protected int count = 0;
            protected int ind = 0;

            @Override
            public synchronized boolean doOutput() {
                try {
                    count++;

                    ContestObjectDelta co = queue.poll();
                    while (co != null) {
                        IContestObject obj = filter.filter(co.obj);
                        if (obj != null) {
                            writer.writeEvent(obj, prefix + (ind++), co.d);
                            count = 0;
                        }
                        co = queue.poll();
                    }
                    pw.flush();
                    if (count > 120) {
                        writer.writeHeartbeat();
                        pw.flush();

                        try {
                            session.getBasicRemote().sendPing(ByteBuffer.allocate(0));
                        } catch (IOException ioe) {
                            Trace.trace(Trace.ERROR, "Error sending WebSocket ping", ioe);
                            remove();
                            return false;
                        }

                        count = 0;
                    }
                    return true;
                } catch (Throwable t) {
                    Trace.trace(Trace.ERROR, "Error writing to event feed", t);
                    remove();
                    return false;
                }
            }

            protected void remove() {
                contest.removeListener(listener);
                cc.remove(session);
                try {
                    session.close(new CloseReason(CloseCodes.UNEXPECTED_CONDITION, "Event feed writer error"));
                } catch (IOException e) {
                    Trace.trace(Trace.ERROR, "Error closing websocket", e);
                }
            }
        });
        contest.addListenerFromStart(listener);
        Trace.trace(Trace.USER, "WebSocket opened for contest " + contestId + " by user " + user);
    }

    @OnClose
    public void onClose(Session session) {
        if (contest != null && listener != null) {
            contest.removeListener(listener);
        }
        if (cc != null) {
            cc.remove(session);
        }
        Trace.trace(Trace.USER, "WebSocket closed");
    }
}
