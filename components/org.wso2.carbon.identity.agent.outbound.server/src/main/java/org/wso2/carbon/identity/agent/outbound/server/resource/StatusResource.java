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
package org.wso2.carbon.identity.agent.outbound.server.resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /**
     * @return 200 OK if the connection is healthy,
     * 500 INTERNAL SERVER ERROR otherwise.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response checkHealth() {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Checking health in identity broker.");
        }
        return Response.status(Response.Status.OK).build();
    }

}
