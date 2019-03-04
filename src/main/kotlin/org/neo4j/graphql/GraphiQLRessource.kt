package org.neo4j.graphql

import javax.ws.rs.*
import javax.ws.rs.core.Response

@Path("ui")
class GraphiQLRessource() {

  @Path("{path:.*}")
  @GET
  fun staticResources(@PathParam("path") path: String): Response {
    val resource = javaClass.getClassLoader().getResourceAsStream(String.format("/WEB-INF/public/%s", path));

    if (resource == null)
        return Response.status(Response.Status.NOT_FOUND).build()
    return Response.ok().entity(resource).build()
  }
}