package org.wso2.carbon.identity.agent.userstore;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.CharsetUtil;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.identity.agent.userstore.constant.CommonConstants;
import org.wso2.carbon.identity.agent.userstore.exception.UserStoreException;
import org.wso2.carbon.identity.agent.userstore.manager.common.UserStoreManager;
import org.wso2.carbon.identity.agent.userstore.manager.common.UserStoreManagerBuilder;

import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Map;
import javax.net.ssl.SSLException;

/**
 * WebSocket Client Handler for Testing.
 */
public class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketClient.class);

    private final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakeFuture;
    private final static int SOCKET_RETRY_INTERVAL = 2000; //Two seconds

    private String textReceived = "";
    private ByteBuffer bufferReceived = null;
    private WebSocketClient client;

    public WebSocketClientHandler(WebSocketClientHandshaker handshaker, WebSocketClient client) {
        this.handshaker = handshaker;
        this.client = client;
    }

    public ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        handshakeFuture = ctx.newPromise();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        handshaker.handshake(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {

        logger.info("Socket Client disconnected!");

        while (true) {

            boolean result = false;
            try {
                logger.info("Trying to reconnect the server...");
                result = client.handhshake();
                Thread.sleep(SOCKET_RETRY_INTERVAL);
            } catch (InterruptedException e) {
                logger.error("Error occurred while reconnecting to socket server", e);
            } catch (URISyntaxException e) {
                logger.error("Error occurred while reconnecting to socket server", e);
            } catch (SSLException e) {
                logger.error("Error occurred while reconnecting to socket server", e);
            }
            if (result) {
                break;
            }
        }

    }

    private void processUserOperationRequest(Channel ch, JSONObject resultObj) throws UserStoreException {
        if (OperationConstants.UM_OPERATION_TYPE_AUTHENTICATE.equals((String) resultObj.get("requestType"))) {

            JSONObject requestObj = resultObj.getJSONObject("requestData");
            UserStoreManager userStoreManager = UserStoreManagerBuilder.getUserStoreManager();
            boolean isAuthenticated = userStoreManager
                    .doAuthenticate(requestObj.getString("username"), requestObj.getString("password"));
            String authenticationResult = OperationConstants.UM_OPERATION_AUTHENTICATE_RESULT_FAIL;
            if (isAuthenticated) {
                authenticationResult = OperationConstants.UM_OPERATION_AUTHENTICATE_RESULT_SUCCESS;
            }
            ch.writeAndFlush(new TextWebSocketFrame(
                    String.format("{correlationId : '%s', responseData: '%s'}",
                            (String) resultObj.get("correlationId"), authenticationResult)));

        } else if (OperationConstants.UM_OPERATION_TYPE_GET_CLAIMS.equals((String) resultObj.get("requestType"))) {

            JSONObject requestObj = resultObj.getJSONObject("requestData");
            String username = requestObj.getString("username");
            String attributes = requestObj.getString("attributes");
            String[] attributeArray = attributes.split(CommonConstants.ATTRIBUTE_LIST_SEPERATOR);
            UserStoreManager userStoreManager = UserStoreManagerBuilder.getUserStoreManager();

            Map<String, String> propertyMap = userStoreManager.getUserPropertyValues(username, attributeArray);
            JSONObject returnObject = new JSONObject(propertyMap);

            logger.info("User Claim values: " + returnObject.toString());
            ch.writeAndFlush(new TextWebSocketFrame(
                    String.format("{correlationId : '%s', responseData: '%s'}",
                            (String) resultObj.get("correlationId"),
                            returnObject.toString())));
        } else if (OperationConstants.UM_OPERATION_TYPE_GET_USER_ROLES
                .equals((String) resultObj.get("requestType"))) {

            JSONObject requestObj = resultObj.getJSONObject("requestData");
            String username = requestObj.getString("username");

            UserStoreManager userStoreManager = UserStoreManagerBuilder.getUserStoreManager();
            String[] roles = userStoreManager.doGetExternalRoleListOfUser(username);
            JSONObject jsonObject = new JSONObject();
            JSONArray usernameArray = new JSONArray(roles);
            jsonObject.put("groups", usernameArray);
            ch.writeAndFlush(new TextWebSocketFrame(
                    String.format("{correlationId : '%s', responseData: '%s'}",
                            (String) resultObj.get("correlationId"),
                            jsonObject.toString())));
        } else if (OperationConstants.UM_OPERATION_TYPE_GET_ROLES.equals((String) resultObj.get("requestType"))) {

            JSONObject requestObj = resultObj.getJSONObject("requestData");
            String limit = requestObj.getString("limit");

            if (limit == null || limit.isEmpty()) {
                limit = String.valueOf(CommonConstants.MAX_USER_LIST);
            }
            UserStoreManager userStoreManager = UserStoreManagerBuilder.getUserStoreManager();
            String[] roleNames = userStoreManager.doGetRoleNames("*", Integer.parseInt(limit));
            JSONObject returnObject = new JSONObject();
            JSONArray usernameArray = new JSONArray(roleNames);
            returnObject.put("groups", usernameArray);

            logger.info("User Claim values: " + returnObject.toString());
            ch.writeAndFlush(new TextWebSocketFrame(
                    String.format("{correlationId : '%s', responseData: '%s'}",
                            (String) resultObj.get("correlationId"),
                            returnObject.toString())));
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        Channel ch = ctx.channel();
        if (!handshaker.isHandshakeComplete()) {
            handshaker.finishHandshake(ch, (FullHttpResponse) msg);
            logger.info("WebSocket Client connected!");
            handshakeFuture.setSuccess();
            return;
        }

        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            throw new IllegalStateException(
                    "Unexpected FullHttpResponse (getStatus=" + response.status() +
                            ", content=" + response.content().toString(CharsetUtil.UTF_8) + ')');
        }

        WebSocketFrame frame = (WebSocketFrame) msg;
        if (frame instanceof TextWebSocketFrame) {
            TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
            JSONObject requestObj = new JSONObject(textFrame.text());

            logger.info("WebSocket Client received text message: " + textFrame.text());
            textReceived = textFrame.text();

            processUserOperationRequest(ch, requestObj);

        } else if (frame instanceof BinaryWebSocketFrame) {
            BinaryWebSocketFrame binaryFrame = (BinaryWebSocketFrame) frame;
            bufferReceived = binaryFrame.content().nioBuffer();
            logger.info("WebSocket Client received  binary message: " + bufferReceived.toString());
        } else if (frame instanceof PongWebSocketFrame) {
            logger.info("WebSocket Client received pong");
            PongWebSocketFrame pongFrame = (PongWebSocketFrame) frame;
            bufferReceived = pongFrame.content().nioBuffer();
        } else if (frame instanceof CloseWebSocketFrame) {
            logger.info("WebSocket Client received closing");
            ch.close();
        }
    }

    /**
     * @return the text received from the server.
     */
    public String getTextReceived() {
        return textReceived;
    }

    /**
     * @return the binary data received from the server.
     */
    public ByteBuffer getBufferReceived() {
        return bufferReceived;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (!handshakeFuture.isDone()) {
            logger.error("Handshake failed : " + cause.getMessage(), cause);
            handshakeFuture.setFailure(cause);
        }
        ctx.close();
    }
}

