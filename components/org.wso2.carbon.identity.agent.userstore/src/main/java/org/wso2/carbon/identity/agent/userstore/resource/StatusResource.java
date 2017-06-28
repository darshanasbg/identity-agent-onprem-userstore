/*
*  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.carbon.identity.agent.userstore.resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.identity.agent.userstore.exception.UserStoreException;
import org.wso2.carbon.identity.agent.userstore.manager.common.UserStoreManager;
import org.wso2.carbon.identity.agent.userstore.manager.common.UserStoreManagerBuilder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *  Connection health check endpoint.
 *  This will be available at https://localhost:8888/wso2agent/status
 */

@Path("/status")
public class StatusResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatusResource.class);
    private static final String SUCCESS_MESSAGE = "{ \"Status\" : \"Success\" }";

    /**
     * @return 200 OK if the connection is healthy,
     * 500 INTERNAL SERVER ERROR otherwise.
     */
    @GET
    @Path("agent/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response checkAgentStatus() {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Checking agent health.");
        }
        return Response.ok(SUCCESS_MESSAGE, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("ldap/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response checkLDAPStatus() {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Checking agent LDAP health.");
        }

        try {
            UserStoreManager userStoreManager = UserStoreManagerBuilder.getUserStoreManager();
            boolean connectionStatus = userStoreManager.getConnectionStatus();
            if (!connectionStatus) {
                LOGGER.error("LDAP health check failed.");
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
            }
        } catch (UserStoreException e) {
            LOGGER.error("LDAP health check failed.", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();

        }

        return Response.ok(SUCCESS_MESSAGE, MediaType.APPLICATION_JSON).build();
    }

}
