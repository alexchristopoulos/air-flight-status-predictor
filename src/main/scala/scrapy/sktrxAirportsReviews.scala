package gr.upatras.ceid.ddcdm.scrapy

import org.jsoup._
import java.io._
import java.util
import org.jsoup.nodes.Element

//This code runs sequentially
object sktrxAirportsReviews {

  def execute() = {

    //fectDataAirportsAirlineQualitites() fetch data for airports from https://airlinequality
    //fetchAllAirportReviewsUrls() fecth data from https://skytrax
    fetchIataCodesFromWiki()
  }

  def fetchIataCodesFromWiki() = {

    val file: File = new File("C:\\Users\\Αλέξανδρος\\Desktop\\iataAirportsCodesWiki.out")
    val bw: BufferedWriter = new BufferedWriter(new FileWriter(file))

    bw.write("IATA,Airport_Name")
    bw.newLine()

    Jsoup
      .connect("https://en.wikipedia.org/wiki/List_of_airports_by_IATA_code:_A")
      .referrer("https://en.wikipedia.org")
      .userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36\"")
      .get()
      .getElementsByClass("wikitable")
      .get(0)
      .getElementsByTag("tr")
      .forEach(tableRow => {

        println(tableRow.getElementsByTag("td").size())

        if (tableRow.getElementsByTag("td").size() == 6) {

          bw.write(tableRow.getElementsByTag("td").get(0).text() + "," + tableRow.getElementsByTag("td").get(2).text())
          bw.newLine()
        }

      })

    bw.close()
  }

  def fectDataAirportsAirlineQualitites() = {

    val urls: util.ArrayList[String] = fetchAllUrsAirQua()

    val file = new File("C:\\Users\\Αλέξανδρος\\Desktop\\dataAirportsAirlinequality.out")
    val bw = new BufferedWriter(new FileWriter(file))

    bw.write("AirportName,Rating_out_of_10,NumOfReviews")
    bw.newLine()

    var counter = 0
    urls.forEach(url => {

      val div_review_info: Element = Jsoup
        .connect(url)
        .referrer("https://skytraxratings.com")
        .userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36\"")
        .get()
        .getElementsByClass("comp_review-header")
        .get(0)
        .getElementsByClass("review-info")
        .get(0)

      val airportName: String = div_review_info
        .getElementsByClass("review-heading")
        .get(0)
        .getElementsByTag("h1")
        .get(0)
        .text()

      val rating: String = div_review_info.getElementsByClass("rating-totals")
        .get(0)
        .getElementsByAttributeValue("itemprop", "ratingValue")
        .get(0)
        .text()

      val numOfRatings: String = div_review_info
        .getElementsByAttributeValue("itemprop", "reviewCount")
        .get(0)
        .text()

      bw.write(airportName + "," + rating + "," + numOfRatings)
      bw.newLine()

      Thread.sleep(50)
      counter += 1
      println(counter.toString() + '/' + urls.size())
    })

    bw.close()
    println("num of airports " + urls.size())
  }


  def fetchAllUrsAirQua(): util.ArrayList[String] = {

    var urls: util.ArrayList[String] = new util.ArrayList[String]

    Jsoup
      .connect("https://www.airlinequality.com/review-pages/a-z-airport-reviews/")
      .referrer("https://skytraxratings.com")
      .userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36\"")
      .get()
      .getElementsByClass("a_z_col_group")
      .forEach(col_group => {
        col_group.getElementsByTag("a").forEach(a => {
          urls.add("https://www.airlinequality.com" + a.attr("href"))
        })
      })

    return urls
  }

  def fetchAllAirportReviewsDataFromSkytrax() = {

    val file = new File("C:\\Users\\Αλέξανδρος\\Desktop\\dataSkytrax.out")
    val bw = new BufferedWriter(new FileWriter(file))

    bw.write("Airport_Name," + "Rank_Out_of_5")
    bw.write("\n")

    Jsoup
      .connect("https://skytraxratings.com/a-z-of-airport-ratings")
      .get()
      .getElementsByTag("tr")
      .forEach(tableRow => {

        val rank: Int = tableRow.getElementsByClass("column-1")
          .get(0)
          .text()
          .toInt

        val airportName: String = tableRow
          .getElementsByTag("a")
          .get(0)
          .text()

        bw.write(airportName + "," + rank)
        bw.write("\n")

      })

    bw.close()
  }
}
