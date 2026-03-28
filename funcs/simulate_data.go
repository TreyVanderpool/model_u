package funcs

import (
	"fmt"
	"time"

	odb "github.com/TreyVanderpool/oliver-golib/db"
	oinit "github.com/TreyVanderpool/oliver-golib/init"
	ol "github.com/TreyVanderpool/oliver-golib/logging"
	osch "github.com/TreyVanderpool/oliver-golib/schwab"
	osql "github.com/TreyVanderpool/oliver-golib/sql"
  ou "github.com/TreyVanderpool/oliver-golib/utils"
)

var (
  __SQLs       osql.SQLs
  liB_TSIdx    int
  liB_SymIdx   int
  liB_TypeIdx  int
  liB_PAIdx    int
  liB_MIIdx    int
  liB_SQIdx    int
  liB_MTIdx    int
)

type EquityEvent struct {
  TranDate              string
  Symbol                string
  AskPrice              float64
  AskSize               int
  BidPrice              float64
  BidSize               int
  AskBidSpread          float64
}

type BookEvent struct {
  TranDate              string
  Symbol                string
  Type                  string
  MarkerName            string
  MarkerTime            string
  Size                  int
  Price                 float64
}

//--------------------------------------------------
// Function: main
//--------------------------------------------------
func SimulateData( asSymbols, asDate string, acLog ol.ILogger, acDB *odb.DB, apEquityFunc func(*osch.SEquityOne), apBookFunc func(*osch.SCRBook) ) {
  __SQLs = oinit.Init( oinit.INIT_SQLS, acDB, acLog ).(osql.SQLs)

  lsCols := "tran_ts,symbol,ask_price,bid_price,ask_size,bid_size"
  lsWhere := fmt.Sprintf( "tran_ts between '%s 00:00:00.000' and '%s 23:59:59.999'", asDate, asDate )
  lcEquities, err := __SQLs.R_StreamEquityBySymbol( asSymbols, lsWhere, lsCols, "tran_ts, symbol" )

  if err != nil {
    acLog.Exception( err )
    acLog.Error( "SQL: %s", lcEquities.SQL )
    return
  }

  lcBooks, err := __SQLs.R_StreamBooksBySymbol( asSymbols, lsWhere, "*", "tran_ts, symbol, type_id, price_amt, marker_time" )

  if err != nil {
    acLog.Exception( err )
    acLog.Error( "SQL: %s", lcBooks.SQL )
    return
  }

  // defer lcEquities.Close()
  // defer lcBooks.Close()

  lbEOFEquities := lcEquities.Next()
  lbEOFBooks := lcBooks.Next()
  lsEquityTS := "2099-12-31"
  lsBookTS := "2099-12-31"
  lcEquityEvent := &osch.SEquityOne{}
  // lcBookEvent := BookEvent{}
  liE_TSIdx := lcEquities.Fields["tran_ts"]
  liE_SymIdx := lcEquities.Fields["symbol"]
  liE_ASIdx := lcEquities.Fields["ask_size"]
  liE_APIdx := lcEquities.Fields["ask_price"]
  liE_BSIdx := lcEquities.Fields["bid_size"]
  liE_BPIdx := lcEquities.Fields["bid_price"]
  liB_TSIdx = lcBooks.Fields["tran_ts"]
  liB_SymIdx = lcBooks.Fields["symbol"]
  liB_TypeIdx = lcBooks.Fields["type_id"]
  liB_PAIdx = lcBooks.Fields["price_amt"]
  liB_MIIdx = lcBooks.Fields["marker_id"]
  liB_SQIdx = lcBooks.Fields["size_qty"]
  liB_MTIdx = lcBooks.Fields["marker_time"]

  // lcEquityTracker := osch.EquityTracker{}
  lcSCRBook := &osch.SCRBook{}

  if ! lbEOFEquities {
    lsEquityTS = lcEquities.Row.Str( liE_TSIdx )
  }


  if ! lbEOFBooks {
    lsBookTS, lcSCRBook, lbEOFBooks = createBook( lcBooks )
  }

  lbEquityEvent := false
  lbBookEvent := false

  for ! lbEOFEquities || ! lbEOFBooks {

    if lsEquityTS == lsBookTS {
      lbEquityEvent = true
      lbBookEvent = true
    } else if lsEquityTS < lsBookTS {
      lbEquityEvent = true
      lbBookEvent = false
    } else {
      lbBookEvent = true
      lbEquityEvent = false
    }

    if lbEquityEvent {
      if apEquityFunc != nil {
        lcEquityEvent = new( osch.SEquityOne )
        lcEquityEvent.TranDate = lcEquities.Row.Str( liE_TSIdx )
        lcEquityEvent.Symbol = lcEquities.Row.Str( liE_SymIdx )
        lcEquityEvent.AskSize = lcEquities.Row.Int( liE_ASIdx )
        lcEquityEvent.AskPrice = lcEquities.Row.Float( liE_APIdx )
        lcEquityEvent.BidSize = lcEquities.Row.Int( liE_BSIdx )
        lcEquityEvent.BidPrice = lcEquities.Row.Float( liE_BPIdx )
        apEquityFunc( lcEquityEvent )
      }
      lbEOFEquities = lcEquities.Next()
      if ! lbEOFEquities {
        lsEquityTS = lcEquities.Row.Str( liE_TSIdx )
      } else {
        lsEquityTS = "2099-12-31"
      }
    }

    if lbBookEvent {
      if apBookFunc != nil {
        apBookFunc( lcSCRBook )
      }
      lsBookTS, lcSCRBook, lbEOFBooks = createBook( lcBooks )
      if ! lbEOFBooks {
        lsBookTS = lcBooks.Row.Str( liB_TSIdx )
      } else {
        lsBookTS = "2099-12-31"
      }
    }
  }

  acLog.Info( "Total Rows: Equities: %d  Books: %d", lcEquities.RowCount, lcBooks.RowCount )
}

func createBook( acResult *osql.OResult ) ( string, *osch.SCRBook, bool ) {
  lbEOF := false
  lcBook := &osch.SCRBook{}
  lcBook.Data.Symbol = acResult.Row.Str( liB_SymIdx )
  lcBook.Data.MarketTimeStr = acResult.Row.Str( liB_TSIdx )
  lcBook.Data.AskSide = make( []osch.SCRBookSide, 0 )
  lcBook.Data.BidSide = make( []osch.SCRBookSide, 0 )

  for ! lbEOF &&
      lcBook.Data.MarketTimeStr == acResult.Row.Str( liB_TSIdx ) &&
      lcBook.Data.Symbol == acResult.Row.Str( liB_SymIdx ) {
      lsType := acResult.Row.Str( liB_TypeIdx )
      lcBS := osch.SCRBookSide{}
      lcBS.Price = acResult.Row.Float( liB_PAIdx )
      for ! lbEOF &&
          lcBS.Price == acResult.Row.Float( liB_PAIdx ) &&
          lsType == acResult.Row.Str( liB_TypeIdx ) &&
          lcBook.Data.MarketTimeStr == acResult.Row.Str( liB_TSIdx ) &&
          lcBook.Data.Symbol == acResult.Row.Str( liB_SymIdx ) {
        lcML := osch.SCRMarkList{}
        lcML.MarkerID = acResult.Row.Str( liB_MIIdx )
        lcML.Size = acResult.Row.Int( liB_SQIdx )
        lcTime, _ := time.Parse( ou.TIMESTAMPFORMAT, "1970-01-01 " + acResult.Row.Str( liB_MTIdx ) )
        lcML.Time = int(lcTime.UnixMilli())
        lcBS.Size += lcML.Size
        lcBS.MarketList = append( lcBS.MarketList, lcML )
        lbEOF = acResult.Next()
      }
      if lsType == "a" {
        lcBook.Data.AskSide = append( lcBook.Data.AskSide, lcBS )
      } else {
        lcBook.Data.BidSide = append( lcBook.Data.BidSide, lcBS )
      }

  }

  return lcBook.Data.MarketTimeStr, lcBook, lbEOF
}