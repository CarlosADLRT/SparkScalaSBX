package com.hansospina.spark

import java.net.URI
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClients
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write


object SBXQuery extends App {


  case class Variety(varietyName: String, color: String, productGroup: String, _KEY: String)

  case class Replacement(variety: String, replace: String)

  case class NewReplace(variety: String, replace: String)

  case class Response[T](success: Boolean, error: Option[String], results: List[T])

  case class Data[T](rows: List[T], row_model: String, domain: String)


  def loadFromSBX(query: String): String = {
    val client = HttpClients.createDefault()
    val uri = new URI(s"https://www.ibuyflowers.com/api/data/v1/row/find")
    val post = new HttpPost(uri)
    post.addHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.toString)
    post.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString)
    post.addHeader(HttpHeaders.AUTHORIZATION, s"Bearer ${scala.util.Properties.envOrElse("TOKEN", "DOOM")}")
    println(post.toString)

    post.setEntity(new StringEntity(query))

    val resp = client.execute(post)

    try {
      val entity = resp.getEntity
      IOUtils.toString(entity.getContent, StandardCharsets.UTF_8)
    } finally {
      resp.close()
    }
  }

  def insertToSbx(query: String): Unit = {
    val client = HttpClients.createDefault()
    val uri = new URI(s"https://sbxcloud.com/api/data/v1/row")
    val post = new HttpPost(uri)
    post.addHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.toString)
    post.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString)
    post.addHeader(HttpHeaders.AUTHORIZATION, s"Bearer ${scala.util.Properties.envOrElse("TOKEN", "DOOM")}")

    post.setEntity(new StringEntity(query))

    val resp = client.execute(post)

    try {
      val entity = resp.getEntity
      IOUtils.toString(entity.getContent, StandardCharsets.UTF_8)
    } finally {
      resp.close()
    }
  }


  /** Our main function where the action happens */

  val jsonVariety1 = parse(loadFromSBX("{\"row_model\":\"variety\",\"domain\":\"129\",\"page\":1,\"size\":1000,\"where\":[{\"ANDOR\":\"AND\",\"GROUP\":[{\"ANDOR\":\"AND\",\"VAL\":[\"618b7a0a-f638-4007-a05d-ba1f22fbad64\",\"12932620-8484-4dba-bd71-205ed8e5da22\",\"b74d7143-dba5-40aa-b6f6-7aeb89c6fce4\",\"47c397da-36d2-4e19-abac-315b05d83a9a\",\"89ea3f9d-8cb9-4ee9-958e-dc6490be0ba9\",\"f6cd25f1-8984-4888-ad4b-90038f2f3670\",\"f6c341ad-8fbd-45d4-8615-26a3caa85d97\"],\"FIELD\":\"product_group\",\"OP\":\"not in\"}]}]}"))
  val jsonVariety2 = parse(loadFromSBX("{\"row_model\":\"variety\",\"domain\":\"129\",\"page\":2,\"size\":1000,\"where\":[{\"ANDOR\":\"AND\",\"GROUP\":[{\"ANDOR\":\"AND\",\"VAL\":[\"618b7a0a-f638-4007-a05d-ba1f22fbad64\",\"12932620-8484-4dba-bd71-205ed8e5da22\",\"b74d7143-dba5-40aa-b6f6-7aeb89c6fce4\",\"47c397da-36d2-4e19-abac-315b05d83a9a\",\"89ea3f9d-8cb9-4ee9-958e-dc6490be0ba9\",\"f6cd25f1-8984-4888-ad4b-90038f2f3670\",\"f6c341ad-8fbd-45d4-8615-26a3caa85d97\"],\"FIELD\":\"product_group\",\"OP\":\"not in\"}]}]}"))
  val jsonVariety3 = parse(loadFromSBX("{\"row_model\":\"variety\",\"domain\":\"129\",\"page\":3,\"size\":1000,\"where\":[{\"ANDOR\":\"AND\",\"GROUP\":[{\"ANDOR\":\"AND\",\"VAL\":[\"618b7a0a-f638-4007-a05d-ba1f22fbad64\",\"12932620-8484-4dba-bd71-205ed8e5da22\",\"b74d7143-dba5-40aa-b6f6-7aeb89c6fce4\",\"47c397da-36d2-4e19-abac-315b05d83a9a\",\"89ea3f9d-8cb9-4ee9-958e-dc6490be0ba9\",\"f6cd25f1-8984-4888-ad4b-90038f2f3670\",\"f6c341ad-8fbd-45d4-8615-26a3caa85d97\"],\"FIELD\":\"product_group\",\"OP\":\"not in\"}]}]}"))
  val jsonVariety4 = parse(loadFromSBX("{\"row_model\":\"variety\",\"domain\":\"129\",\"page\":4,\"size\":1000,\"where\":[{\"ANDOR\":\"AND\",\"GROUP\":[{\"ANDOR\":\"AND\",\"VAL\":[\"618b7a0a-f638-4007-a05d-ba1f22fbad64\",\"12932620-8484-4dba-bd71-205ed8e5da22\",\"b74d7143-dba5-40aa-b6f6-7aeb89c6fce4\",\"47c397da-36d2-4e19-abac-315b05d83a9a\",\"89ea3f9d-8cb9-4ee9-958e-dc6490be0ba9\",\"f6cd25f1-8984-4888-ad4b-90038f2f3670\",\"f6c341ad-8fbd-45d4-8615-26a3caa85d97\"],\"FIELD\":\"product_group\",\"OP\":\"not in\"}]}]}"))
  val jsonVariety5 = parse(loadFromSBX("{\"row_model\":\"variety\",\"domain\":\"129\",\"page\":5,\"size\":1000,\"where\":[{\"ANDOR\":\"AND\",\"GROUP\":[{\"ANDOR\":\"AND\",\"VAL\":[\"618b7a0a-f638-4007-a05d-ba1f22fbad64\",\"12932620-8484-4dba-bd71-205ed8e5da22\",\"b74d7143-dba5-40aa-b6f6-7aeb89c6fce4\",\"47c397da-36d2-4e19-abac-315b05d83a9a\",\"89ea3f9d-8cb9-4ee9-958e-dc6490be0ba9\",\"f6cd25f1-8984-4888-ad4b-90038f2f3670\",\"f6c341ad-8fbd-45d4-8615-26a3caa85d97\"],\"FIELD\":\"product_group\",\"OP\":\"not in\"}]}]}"))
  val jsonVariety6 = parse(loadFromSBX("{\"row_model\":\"variety\",\"domain\":\"129\",\"page\":6,\"size\":1000,\"where\":[{\"ANDOR\":\"AND\",\"GROUP\":[{\"ANDOR\":\"AND\",\"VAL\":[\"618b7a0a-f638-4007-a05d-ba1f22fbad64\",\"12932620-8484-4dba-bd71-205ed8e5da22\",\"b74d7143-dba5-40aa-b6f6-7aeb89c6fce4\",\"47c397da-36d2-4e19-abac-315b05d83a9a\",\"89ea3f9d-8cb9-4ee9-958e-dc6490be0ba9\",\"f6cd25f1-8984-4888-ad4b-90038f2f3670\",\"f6c341ad-8fbd-45d4-8615-26a3caa85d97\"],\"FIELD\":\"product_group\",\"OP\":\"not in\"}]}]}"))
  val jsonReplacement = parse(loadFromSBX("{\"row_model\":\"replacement\",\"domain\":\"129\",\"page\":1,\"size\":1000}"))

  implicit val formats: DefaultFormats.type = DefaultFormats

  val resVariety1 = jsonVariety1.camelizeKeys.extract[Response[Variety]].results
  val resVariety2 = jsonVariety2.camelizeKeys.extract[Response[Variety]].results
  val resVariety3 = jsonVariety3.camelizeKeys.extract[Response[Variety]].results
  val resVariety4 = jsonVariety4.camelizeKeys.extract[Response[Variety]].results
  val resVariety5 = jsonVariety5.camelizeKeys.extract[Response[Variety]].results
  val resVariety6 = jsonVariety6.camelizeKeys.extract[Response[Variety]].results

  val resVariety = resVariety1 ++ resVariety2 ++ resVariety3 ++ resVariety4 ++ resVariety5 ++ resVariety6

  val resReplacement = jsonReplacement.camelizeKeys.extract[Response[Replacement]]

  if (resReplacement.success) {
    val groupBy = resVariety.groupBy(a => (a.color, a.productGroup)).filter(a => a._2.size > 1)
    val newGroup = groupBy.values.flatMap(a => {
      val x = a.flatMap(b => {
        var index = a.indexOf(b) + 1
        var array: List[Replacement] = List()
        while (index < a.size) {
          array = new Replacement(b._KEY, a(index)._KEY) :: array
          index += 1
        }
        array
      })
      x
    })

    println(newGroup.size)
    println(resReplacement.results.length)
    println(newGroup.size - resReplacement.results.length)
    val tmp1 = new Data[Replacement](newGroup.filterNot(resReplacement.results.toSet).slice(0, 1000).toList, "replacement", "129")
    val tmp2 = new Data[Replacement](newGroup.filterNot(resReplacement.results.toSet).slice(1001, 2000).toList, "replacement", "129")
    val tmp3 = new Data[Replacement](newGroup.filterNot(resReplacement.results.toSet).slice(2001, 1000).toList, "replacement", "129")
    println(newGroup.filterNot(resReplacement.results.toSet).size)

  } else {
    println(resReplacement.error.getOrElse("No error but didn't work dude."))
  }

}
