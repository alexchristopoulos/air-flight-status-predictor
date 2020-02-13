package gr.upatras.ceid.ddcdm.scrapy;

import org.jsoup._
import java.io._

object tripAdvAirlineCarriersReviews {

  private val numOfPages:Int = 63;

  def extractReviews(): Unit ={

    val file = new File("C:\\Users\\Αλέξανδρος\\Desktop\\data.out")
    val bw = new BufferedWriter(new FileWriter(file))

    for (i <- 0 to numOfPages-1){

      val url:String = "https://www.tripadvisor.com/MetaPlacementAjax?placementName=airlines_lander_main&wrap=true&skipLocation=true&page=" + i.toString() + "&sort=alphabetical"

      Jsoup.connect(url)
        .get()
        .getElementsByClass("prw_airlines_airline_lander_card")
        .forEach(airlineCarrierCard => {

          val iata = airlineCarrierCard.getElementsByClass("airlineData").attr("data-iata")
          val carrier = airlineCarrierCard.getElementsByClass("airlineName").attr("data-name")
          val rating = airlineCarrierCard.getElementsByClass("ui_bubble_rating").attr("alt")
          var numOfReviews = airlineCarrierCard.getElementsByClass("airlineReviews").html()

          if(numOfReviews=="")
            numOfReviews="0"
          else
            numOfReviews = numOfReviews.trim().split(" ")(0).replace(",", ".")

          bw.write(iata + "," + carrier + "," + rating + "," + numOfReviews)
          bw.write("\n")

      });

      println("Page " + i.toString() + " out of " + numOfPages.toString())
    }

    bw.close()
  }
}
