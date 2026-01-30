# ExamenProgramacion
package clases.semana12.RedesSociales

import cats.effect.{IO, IOApp}
import fs2.{Stream, text}
import fs2.io.file.{Files, Path}
import doobie._
import doobie.implicits._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class RedesSociales(
  id: Int,
  nombreUsuario: String,
  plataforma: String,
  seguidores: Int,
  siguiendo: Int,
  publicaciones: Int,
  meGustaPromedio: Int,
  comentariosPromedio: Int,
  esInfluencer: Boolean,
  fechaRegistro: LocalDate
)

object RedesSocialesOracle extends IOApp.Simple {

  private val rutaCSV = "redes_sociales.csv"
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  private val xa = Transactor.fromDriverManager[IO](
    "oracle.jdbc.OracleDriver",
    "jdbc:oracle:thin:@localhost:1521:xe",
    "MY_DB",
    "mydb"
  )

  private def parseLine(line: String): Option[RedesSociales] = {
    val cols = line.split(",").map(_.trim)
    if (cols.length < 10 || cols(0) == "ID") None
    else try {
      Some(RedesSociales(
        cols(0).toInt,
        cols(1),
        cols(2),
        cols(3).toInt,
        cols(4).toInt,
        cols(5).toInt,
        cols(6).toInt,
        cols(7).toInt,
        cols(8).toLowerCase == "true",
        LocalDate.parse(cols(9), dateFormatter)
      ))
    } catch { case _: Exception => None }
  }

  private def insert(rs: RedesSociales): ConnectionIO[Int] = {
    val inf = if (rs.esInfluencer) "1" else "0"
    sql"""
      INSERT INTO REDES_SOCIALES 
      (ID, NOMBRE_USUARIO, PLATAFORMA, SEGUIDORES, SIGUIENDO, PUBLICACIONES, 
       ME_GUSTA_PROMEDIO, COMENTARIOS_PROMEDIO, ES_INFLUENCER, FECHA_REGISTRO)
      VALUES (${rs.id}, ${rs.nombreUsuario}, ${rs.plataforma}, ${rs.seguidores}, 
              ${rs.siguiendo}, ${rs.publicaciones}, ${rs.meGustaPromedio}, 
              ${rs.comentariosPromedio}, $inf, ${java.sql.Date.valueOf(rs.fechaRegistro)})
    """.update.run
  }

  override def run: IO[Unit] = {
    Files[IO]
      .readAll(Path(rutaCSV))
      .through(text.utf8.decode)
      .through(text.lines)
      .drop(1)
      .map(parseLine)
      .collect { case Some(r) => r }
      .evalMap(r => insert(r).transact(xa).attempt.flatMap {
        case Right(_) => IO.println(s"OK: ${r.nombreUsuario}")
        case Left(e)  => IO.println(s"Error: ${e.getMessage}")
      })
      .compile
      .drain
  }
}
