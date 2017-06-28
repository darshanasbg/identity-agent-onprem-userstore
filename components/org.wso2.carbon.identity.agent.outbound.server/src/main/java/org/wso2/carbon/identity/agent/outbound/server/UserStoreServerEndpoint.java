/*
 *   Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.wso2.carbon.identity.agent.outbound.server;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.identity.agent.outbound.server.dao.AgentMgtDao;
import org.wso2.carbon.identity.agent.outbound.server.dao.TokenMgtDao;
import org.wso2.carbon.identity.agent.outbound.server.model.MessageBrokerConfig;
import org.wso2.carbon.identity.agent.outbound.server.util.ServerConfigurationBuilder;
import org.wso2.carbon.identity.user.store.common.MessageRequestUtil;
import org.wso2.carbon.identity.user.store.common.UserStoreConstants;
import org.wso2.carbon.identity.user.store.common.messaging.JMSConnectionException;
import org.wso2.carbon.identity.user.store.common.messaging.JMSConnectionFactory;
import org.wso2.carbon.identity.user.store.common.model.AccessToken;
import org.wso2.carbon.identity.user.store.common.model.UserOperation;
import org.wso2.carbon.kernel.utils.StringUtils;

import java.io.IOException;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.websocket.CloseReason;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;

/**
 * Web socket server endpoint
 */
@ServerEndpoint(value = "/server/{node}")
public class UserStoreServerEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserStoreServerEndpoint.class);
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private SessionHandler serverHandler;
    private String serverNode;

    public UserStoreServerEndpoint(SessionHandler serverHandler, String serverNode) {
        this.serverHandler = serverHandler;
        this.serverNode = serverNode;
        initializeConnections();
    }

    /**
     * Initializing all the agent connection established with server node.
     */
    private void initializeConnections() {
        AgentMgtDao agentMgtDao = new AgentMgtDao();
        agentMgtDao.updateConnectionStatus(serverNode, UserStoreConstants.CLIENT_CONNECTION_STATUS_CONNECTION_FAILED);
    }

    /**
     * Process response message and send to response queue.
     * @param message Message
     */
    private void processResponse(String message) {

        JMSConnectionFactory connectionFactory = new JMSConnectionFactory();
        Connection connection = null;
        MessageProducer producer;
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Start processing response message: " + message);
            }
            MessageBrokerConfig conf = ServerConfigurationBuilder.build().getMessagebroker();
            connectionFactory.createActiveMQConnectionFactory(conf.getUrl());
            connection = connectionFactory.createConnection();
            connectionFactory.start(connection);
            javax.jms.Session session = connectionFactory.createSession(connection);
            Destination responseQueue = connectionFactory
                    .createQueueDestination(session, UserStoreConstants.QUEUE_NAME_RESPONSE);
            producer = connectionFactory.createMessageProducer(session, responseQueue, DeliveryMode.NON_PERSISTENT);
            producer.setTimeToLive(UserStoreConstants.QUEUE_SERVER_MESSAGE_LIFETIME);

            JSONObject resultObj = new JSONObject(message);
            String responseData = resultObj.get(UserStoreConstants.UM_JSON_ELEMENT_RESPONSE_DATA).toString();
            String correlationId = (String) resultObj
                    .get(UserStoreConstants.UM_JSON_ELEMENT_REQUEST_DATA_CORRELATION_ID);

            UserOperation responseOperation = new UserOperation();
            responseOperation.setCorrelationId(correlationId);
            responseOperation.setResponseData(responseData.toString());

            ObjectMessage responseMessage = session.createObjectMessage();
            responseMessage.setObject(responseOperation);
            responseMessage.setJMSCorrelationID(correlationId);
            producer.send(responseMessage);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Finished processing response message: " + message);
            }
        } catch (JMSException e) {
            LOGGER.error("Error occurred while sending message: " + message, e);
        } catch (JSONException e) {
            LOGGER.error("Error occurred while reading json payload of message: " + message, e);
        } catch (JMSConnectionException e) {
            LOGGER.error("Error occurred while creating JMS connection to send message: " + message, e);
        } finally {
            try {
                connectionFactory.closeConnection(connection);
            } catch (JMSConnectionException e) {
                LOGGER.error("Error occurred while closing JMS connection", e);
            }
        }
    }

    /**
     * Get access token header "accesstoken" from user properties
     * @param userProperties User properties
     * @return Access token
     */
    private String getAccessTokenFromUserProperties(Map<String, Object> userProperties) {
        String authorizationHeader = (String) userProperties.get(AUTHORIZATION_HEADER);
        if (!StringUtils.isNullOrEmpty(authorizationHeader)) {
            String[] splitValues = authorizationHeader.trim().split(" ");
            if (splitValues.length == 2) {
                return splitValues[1];
            }
        }
        return null;
    }

    @OnOpen
    public void onOpen(@PathParam("node") String node, Session session) {
        handleSession(getAccessTokenFromUserProperties(session.getUserProperties()), node, session);
    }

    /**
     * Handle session
     * @param token access token
     * @param node Client node
     * @param session web socket session
     */
    private void handleSession(String token, String node, Session session) {

        LOGGER.info("Client: " + node + " trying to connect the sever.");

        if (StringUtils.isNullOrEmpty(token)) {
            try {
                String message = "Closing session from node: " + node + " due to invalid access token.";
                sendErrorMessage(session, message);
            } catch (IOException | JSONException e) {
                LOGGER.error("Error occurred while closing session with client node: " + node);
            }
            return;
        }
        TokenMgtDao tokenMgtDao = new TokenMgtDao();
        AccessToken accessToken = tokenMgtDao.getAccessToken(token);
        ConnectionHandler connectionHandler = new ConnectionHandler();
        if (accessToken == null || !UserStoreConstants.ACCESS_TOKEN_STATUS_ACTIVE.equals(accessToken.getStatus())) {
            try {
                String message = "Closing session with node: " + node + " due to invalid access token.";
                sendErrorMessage(session, message);
            } catch (IOException | JSONException e) {
                LOGGER.error("Error occurred while closing session with node: " + node, e);
            }
        } else if (connectionHandler.isNodeConnected(accessToken, node)) {
            try {
                String message = "Client: " + node
                        + " already connected. This may be an inconsistency of the server notification of agent";
                sendErrorMessage(session, message);
            } catch (IOException | JSONException e) {
                LOGGER.error("Error occurred while closing session with node: " + node, e);
            }
        } else if (connectionHandler.isConnectionLimitExceed(accessToken.getTenant(), accessToken.getDomain())) {
            try {
                String message = "No of agent connections limit exceeded for tenant: " + accessToken.getTenant();
                sendErrorMessage(session, message);
            } catch (IOException | JSONException e) {
                LOGGER.error("Error occurred while closing session with node: " + node, e);
            }
        } else {
            connectionHandler.addConnection(accessToken, node, serverNode);
            serverHandler.addSession(accessToken.getTenant(), accessToken.getDomain(), session);
            String msg = node + " from " + accessToken.getTenant() + " connected to server node: " + serverNode;
            LOGGER.info(msg);
        }
    }

    /**
     * Send error message to client
     * @param session web socket session
     * @param message Error message
     * @throws IOException
     */
    private void sendErrorMessage(Session session, String message) throws IOException, JSONException {
        LOGGER.error(message);
        UserOperation userOperation = new UserOperation();
        userOperation.setRequestType(UserStoreConstants.UM_OPERATION_TYPE_ERROR);
        JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("message", message);
        userOperation.setRequestData(jsonMessage.toString());
        session.getBasicRemote().sendText(MessageRequestUtil.getUserOperationJSONMessage(userOperation));
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            LOGGER.error("Error occurred while sleep before close session");
        }
        session.close();
    }

    @OnMessage
    public void onTextMessage(String text, Session session) throws IOException {
        //Use thread executor
        Thread loop = new Thread(() -> processResponse(text));
        loop.start();
    }

    @OnMessage
    public void onBinaryMessage(byte[] bytes, Session session) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Reading binary Message");
        }
    }

    @OnClose
    public void onClose(@PathParam("node") String node, CloseReason closeReason, Session session) {

        TokenMgtDao tokenMgtDao = new TokenMgtDao();
        AccessToken accessToken = tokenMgtDao
                .getAccessToken(getAccessTokenFromUserProperties(session.getUserProperties()));

        LOGGER.info("Connection close triggered with status code : " + closeReason.getCloseCode().getCode()
                + " on reason " + closeReason.getReasonPhrase() + " from " + node + " in tenant " + accessToken
                .getTenant());

        if (accessToken != null) {
            serverHandler.removeSession(accessToken.getTenant(), accessToken.getDomain(), session);
            AgentMgtDao agentMgtDao = new AgentMgtDao();
            agentMgtDao.updateConnection(accessToken.getId(), node, serverNode,
                    UserStoreConstants.CLIENT_CONNECTION_STATUS_CONNECTION_FAILED);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Connection close for tenant: " + accessToken.getTenant());
                String msg = node + " from " + accessToken.getTenant() + " disconnected from server node: " +
                        serverNode;
                LOGGER.info(msg);
            }
        }
    }

    @OnError
    public void onError(Throwable throwable, Session session) {
        LOGGER.error("Error found in method : " + throwable.toString());
    }

}
