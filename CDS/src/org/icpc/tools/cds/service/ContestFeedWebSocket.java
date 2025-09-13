package org.icpc.tools.cds.service;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.List;

import org.icpc.tools.cds.CDSConfig;
import org.icpc.tools.cds.ConfiguredContest;
import org.icpc.tools.cds.service.ContestFeedExecutor.Feed;
import org.icpc.tools.cds.service.ContestObjectQueue.ContestObjectDelta;
import org.icpc.tools.cds.presentations.WebSocketConfig;
import org.icpc.tools.contest.Trace;
import org.icpc.tools.contest.model.IContest;
import org.icpc.tools.contest.model.IContestListener;
import org.icpc.tools.contest.model.IContestObject;
import org.icpc.tools.contest.model.feed.NDJSONFeedWriter;
import org.icpc.tools.contest.model.internal.Contest;

import jakarta.websocket.CloseReason;
import jakarta.websocket.CloseReason.CloseCodes;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.PathParam;
import jakarta.websocket.server.ServerEndpoint;

@ServerEndpoint(value = "/api/contests/{contestId}/event-feed/ws", configurator = WebSocketConfig.class)
public class ContestFeedWebSocket {
    private Contest contest;
    private ConfiguredContest cc;
    private IContestListener listener;
    private static final int TRACE_CHARS = 120;

    private static int getEventIndexFromParameter(Session session, IContest contest, String param) {
        List<String> list = session.getRequestParameterMap().get(param);
        if (list == null || list.isEmpty())
            return -1;
        String idVal = list.get(0);
        String prefix = NDJSONFeedWriter.getContestPrefix(contest);
        if (idVal == null || !idVal.startsWith(prefix))
            return -2;
        try {
            return Integer.parseInt(idVal.substring(3)) + 1;
        } catch (Exception e) {
            return -2;
        }
    }

    private static int getEventIndex(Session session, IContest contest) {
        int ind = getEventIndexFromParameter(session, contest, "since_token");
        if (ind == -1)
            ind = getEventIndexFromParameter(session, contest, "since_id");
        if (ind == -1)
            ind = 0;
        return ind;
    }

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
        PrintWriter pw = new PrintWriter(new WebSocketLineWriter(session), true);

        int ind = getEventIndex(session, contest);
        if (ind == -2) {
            try {
                session.close(new CloseReason(CloseCodes.UNEXPECTED_CONDITION, "Invalid event id"));
                return;
            } catch (IOException e) {
                Trace.trace(Trace.ERROR, "Error closing websocket", e);
            }
        }
        final NDJSONFeedWriter writer = new NDJSONFeedWriter(pw);
        final String prefix = NDJSONFeedWriter.getContestPrefix(contest);
        final ContestObjectQueue queue = new ContestObjectQueue(ind);
        listener = (contest2, obj, delta) -> queue.add(obj, delta);
        cc.add(session);
        cc.incrementFeed();
        cc.incrementWS();

        ContestFeedExecutor.getInstance().addFeedSource(new Feed() {
            protected int count = 0;
            protected int ind3 = ind;

            @Override
            public synchronized boolean doOutput() {
                try {
                    count++;

                    ContestObjectDelta co = queue.poll();
                    while (co != null) {
                        IContestObject obj = filter.filter(co.obj);
                        if (obj != null) {
                            writer.writeEvent(obj, prefix + (ind3++), co.d);
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
                    t.printStackTrace();
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
    public void onClose(Session session, CloseReason reason) {
        if (contest != null && listener != null) {
            contest.removeListener(listener);
        }
        if (cc != null) {
            cc.remove(session);
        }
        if (reason != null) {
            Trace.trace(Trace.USER, "WebSocket closed: code=" + reason.getCloseCode().getCode() + ", reason="
                    + reason.getReasonPhrase());
        } else {
            Trace.trace(Trace.USER, "WebSocket closed");
        }
    }

    @OnMessage
    public void onMessage(Session session, String message) {
        String s = message;
        if (s.length() > TRACE_CHARS)
            s = s.substring(0, TRACE_CHARS) + "...";
        Trace.trace(Trace.INFO, session.getId() + " " + s);
    }

    @OnError
    public void onError(Session session, Throwable t) {
        Trace.trace(Trace.ERROR, "WebSocket error", t);
    }
}
