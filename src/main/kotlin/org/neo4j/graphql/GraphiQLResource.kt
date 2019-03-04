package org.neo4j.graphql

import javax.ws.rs.*
import javax.ws.rs.core.Response

@Path("ui")
class GraphiQLResource() {

  private fun render(path: String): Response {
    val res = String.format("/WEB-INF/public/%s", path)
    val resource = GraphiQLResource::class.java.getResourceAsStream(res);

    if (resource == null)
        return Response.status(Response.Status.NOT_FOUND).build()
    return Response.ok().entity(resource).build()
  }

  @Path("{path:.*}")
  @GET
  fun staticResources(@PathParam("path") path: String): Response {
    return render(path)
  }

  @Path("")
  @GET
  fun staticResourcesIndex(): Response {
    return render("index.html")
  }
}