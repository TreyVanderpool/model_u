package funcs

import (
  "fmt"

  osql "github.com/TreyVanderpool/oliver-golib/sql"
  ol "github.com/TreyVanderpool/oliver-golib/logging"
	odb "github.com/TreyVanderpool/oliver-golib/db"
	oinit "github.com/TreyVanderpool/oliver-golib/init"
)

var (
  __SQLs       osql.SQLs
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
func SimulateData( asSymbols, asDate string, acLog ol.ILogger, acDB *odb.DB, apEquityFunc func(EquityEvent), apBookFunc func(BookEvent) ) {
  __SQLs = oinit.Init( oinit.INIT_SQLS, acDB, acLog ).(osql.SQLs)

  lsCols := "tran_ts,symbol,ask_price,bid_price,ask_size,bid_size"
  lsWhere := fmt.Sprintf( "tran_ts between '%s 00:00:00.000' and '%s 23:59:59.999'", asDate, asDate )
  lcEquities, err := __SQLs.R_StreamEquityBySymbol( asSymbols, lsWhere, lsCols, "tran_ts, symbol" )

  if err != nil {
    acLog.Exception( err )
    acLog.Error( "SQL: %s", lcEquities.SQL )
    return
  }

  lcBooks, err := __SQLs.R_StreamBooksBySymbol( asSymbols, lsWhere, "*", "tran_ts, symbol" )

  if err != nil {
    acLog.Exception( err )
    acLog.Error( "SQL: %s", lcBooks.SQL )
    return
  }

  defer lcEquities.Close()
  defer lcBooks.Close()

  lbEOFEquities := lcEquities.Next()
  lbEOFBooks := lcBooks.Next()
  lsEquityTS := "2099-12-31"
  lsBookTS := "2099-12-31"
  lcEquityEvent := EquityEvent{}
  lcBookEvent := BookEvent{}
  liE_TSIdx := lcEquities.Fields["tran_ts"]
  liE_SymIdx := lcEquities.Fields["symbol"]
  liE_ASIdx := lcEquities.Fields["ask_size"]
  liE_APIdx := lcEquities.Fields["ask_price"]
  liE_BSIdx := lcEquities.Fields["bid_size"]
  liE_BPIdx := lcEquities.Fields["bid_price"]
  liB_TSIdx := lcBooks.Fields["tran_ts"]
  liB_SymIdx := lcBooks.Fields["symbol"]
  liB_TypeIdx := lcBooks.Fields["type_id"]
  liB_PAIdx := lcBooks.Fields["price_amt"]
  liB_MIIdx := lcBooks.Fields["marker_id"]
  liB_SQIdx := lcBooks.Fields["size_qty"]
  liB_MTIdx := lcBooks.Fields["marker_time"]

  if ! lbEOFEquities {
    lsEquityTS = lcEquities.Row.Str( liE_TSIdx )
  }

  if ! lbEOFBooks {
    lsBookTS = lcBooks.Row.Str( liB_TSIdx )
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
        lcBookEvent.TranDate = lcBooks.Row.Str( liB_TSIdx )
        lcBookEvent.Symbol = lcBooks.Row.Str( liB_SymIdx )
        lcBookEvent.Type = lcBooks.Row.Str( liB_TypeIdx )
        lcBookEvent.Price = lcBooks.Row.Float( liB_PAIdx )
        lcBookEvent.MarkerName = lcBooks.Row.Str( liB_MIIdx )
        lcBookEvent.Size = lcBooks.Row.Int( liB_SQIdx )
        lcBookEvent.MarkerTime = lcBooks.Row.Str( liB_MTIdx )
        apBookFunc( lcBookEvent )
      }
      lbEOFBooks = lcBooks.Next()
      if ! lbEOFBooks {
        lsBookTS = lcBooks.Row.Str( liB_TSIdx )
      } else {
        lsBookTS = "2099-12-31"
      }
    }
  }
  // }

  // acLog.Info( "SQL: %s", lcEquities.SQL )
  acLog.Info( "Total Rows: Equities: %d  Books: %d", lcEquities.RowCount, lcBooks.RowCount )
}