package funcs

import (
	// "fmt"
	"strings"
	// "time"

	osch "github.com/TreyVanderpool/oliver-golib/schwab"
	// ou "github.com/TreyVanderpool/oliver-golib/utils"
	odb "github.com/TreyVanderpool/oliver-golib/db"
	ol "github.com/TreyVanderpool/oliver-golib/logging"
)

const (
//   FIELDS      string = "symbol,bid_price,ask_price,last_price,open_price"
  FIELDS      string = "*"
  END_TIME    string = "15:00:00"
)

//------------------------------------------------------------
// Function: StartStreaming
// This lets me see the activity for the symbols being
// watched for this execution.
//------------------------------------------------------------
func StartEquityStreaming( asSymbols []string, acLog ol.ILogger, acDB *odb.DB, acSchwab *osch.SCHWAB, asStreamHostName, asEndOfDayTime string ) {
  if len( asSymbols ) == 0 { return }

  lcStreamClient := osch.NewStreamClient()
  lcStreamClient.L = acLog
  lcStreamClient.DB = acDB
  lcStreamClient.HostName = asStreamHostName
  lcStreamClient.AcctNbr = acSchwab.GetAccountNbr()
  lcStreamClient.Keys = strings.Join( asSymbols, "," )
  lcStreamClient.Fields = FIELDS
  lcStreamClient.SetStoreEquityInDB( true )
  lcStreamClient.Connect()
  lsConnectID := lcStreamClient.GetID()
  err := lcStreamClient.EquityOne( EquityEventFunction )

  if err != nil {
    acLog.Exception( err )
    return
  }

  acLog.Info( "Activated streaming for: %s : %s", lsConnectID, strings.Join( asSymbols, "," ) )
  
  defer acLog.Info( "Exiting streaming" )

  lcStreamClient.WaitForClose( asEndOfDayTime )
}

//------------------------------------------------------------
// Function: StartStreaming
// This lets me see the activity for the symbols being
// watched for this execution.
//------------------------------------------------------------
func StartBookStreaming( asSymbols []string, acLog ol.ILogger, acDB *odb.DB, acSchwab *osch.SCHWAB, asStreamHostName, asBookType, asEndOfDayTime string ) {
  var err          error

  if len( asSymbols ) == 0 { return }

  lcStreamClient := osch.NewStreamClient()
  lcStreamClient.L = acLog
  lcStreamClient.DB = acDB
  lcStreamClient.HostName = asStreamHostName
  lcStreamClient.AcctNbr = acSchwab.GetAccountNbr()
  lcStreamClient.Keys = strings.Join( asSymbols, "," )
  lcStreamClient.Fields = "*"
  lcStreamClient.SetStoreBooksInDB( true )
  lcStreamClient.Connect()
  lsConnectID := lcStreamClient.GetID()

  if( asBookType == "nyse" ) {
    err = lcStreamClient.BookNYSE( BookEventNYSE )
  } else {
    err = lcStreamClient.BookNASDAQ( BookEventNASDAQ )
  }

  if err != nil {
    acLog.Exception( err )
    return
  }

  acLog.Info( "Activated book streaming for: %s : %s : %s", asBookType, lsConnectID, strings.Join( asSymbols, "," ) )
  
  // lcRunUntil := ou.GetDurationFromTime( END_TIME, time.Duration( 10 * time.Minute ) )
  defer acLog.Info( "Exiting streaming" )

  lcStreamClient.WaitForClose( asEndOfDayTime )
}

//--------------------------------------------------------------------
// Function: EquityEvent
//--------------------------------------------------------------------
func EquityEventFunction( acEquity osch.SEquityOne, acError error ) {
//   lcHit, lbFnd := gcPicksPlaying[acEquity.Symbol]

//   if ! lbFnd { return }

//   if acEquity.OpenPrice > 0 {
//     lcHit.OpeningPrice = acEquity.OpenPrice
//     Log.Info( "OPEN: %-6s  Ask: %7.2f  Bid: %7.2f  Last: %7.2f  Open: %7.2f",
//               acEquity.Symbol,
//               acEquity.AskPrice,
//               acEquity.BidPrice,
//               acEquity.LastPrice,
//               acEquity.OpenPrice )
//     lcTrans, err := gcSQLs.S_ATranAllOpenSymbol( *gsVersionName, acEquity.Symbol )
//     if err == nil {
//       for _, lTran := range lcTrans {
//         gcSQLs.U_ATranPurchasePrice( lTran.ID, time.Now().Format( ou.TIMESTAMPFORMAT ), acEquity.OpenPrice )
//       }
//     } else {
//       Log.Exception( err )
//     }
//   }

//   if lcHit.OpeningPrice == 0 {
//     return
//   }

//   // When the SELL price > than the opening price
//   // then turn on the Stop Trialing Percent.
//   // If in LIVE move we can create an order to do
//   // this for us.
//   if acEquity.BidPrice > lcHit.OpeningPrice && lcHit.TrailingPrice == 0 {
//     lcHit.TrailingPrice = acEquity.BidPrice - (acEquity.BidPrice * *gfStopTrailingPct)
//     Log.Info( "TRAILING PCT: %-6s : Open: %7.2f  Bid: %7.2f  Open/Bid Diff: %6.2f%%  Trail: %5.2f -> %7.2f",
//               acEquity.Symbol,
//               lcHit.OpeningPrice,
//               acEquity.BidPrice,
//               ou.PctChg( lcHit.OpeningPrice, acEquity.BidPrice ),
//               *gfStopTrailingPct * 100,
//               lcHit.TrailingPrice )
//   }

//   // Check to see if we need to monitor and move the stop trailing percent.
//   if lcHit.TrailingPrice > 0 {
//     if acEquity.BidPrice > 0 && acEquity.BidPrice < lcHit.TrailingPrice {
//       Log.Info( "DROPPED below trailing pct: %-6s : Bid: %7.2f  Trail: %7.2f, selling this position",
//                  acEquity.Symbol, acEquity.BidPrice, lcHit.TrailingPrice )
//       delete( gcPicksPlaying, acEquity.Symbol )
//       SellEquity( acEquity.Symbol, acEquity.AskPrice, acEquity.BidPrice, acEquity.LastPrice, acEquity.OpenPrice )
//       SendSellText( acEquity.Symbol, lcHit.OpeningPrice, acEquity.BidPrice )
//     } else {
//       lfTrailPrice := acEquity.BidPrice - (acEquity.BidPrice * *gfStopTrailingPct)
//       if lfTrailPrice > lcHit.TrailingPrice {
//         lcHit.TrailingPrice = lfTrailPrice
//         Log.Info( "TRAILING PCT: %-6s : Open: %7.2f  Bid: %7.2f  Open/Bid Diff: %6.2f%%  Trail: %5.2f -> %7.2f",
//                   acEquity.Symbol,
//                   lcHit.OpeningPrice,
//                   acEquity.BidPrice,
//                   ou.PctChg( lcHit.OpeningPrice, acEquity.BidPrice ),
//                   *gfStopTrailingPct * 100,
//                   lcHit.TrailingPrice )
//       }
//     }
//   }
}

// //--------------------------------------------------------------------
// // Function: SellEquity
// //--------------------------------------------------------------------
// func SellEquity( asSymbol string, afAskPrice, afBidPrice, afLastPrice, afOpenPrice float64 ) {
//   if *gbLiveOrders { return }

//   lcTrans, err := gcSQLs.S_ATranAllOpenSymbol( *gsVersionName, asSymbol )

//   if err != nil {
//     Log.Exception( err )
//     return
//   }

//   for _, lTran := range lcTrans {
//     gcSQLs.U_ATranSetSell( lTran.ID,
//                           time.Now().Format( ou.TIMESTAMPFORMAT ),
//                           afBidPrice,
//                           lTran.PurchaseQty )
//   }
// }

//--------------------------------------------------------------------
// Function: EquityEvent
//--------------------------------------------------------------------
func BookEventNYSE( acBook osch.SCRBook, acError error ) {
  // Log.Info( "NYSE: BOOK: %+v", acBook )
}

//--------------------------------------------------------------------
// Function: EquityEvent
//--------------------------------------------------------------------
func BookEventNASDAQ( acBook osch.SCRBook, acError error ) {
  // Log.Info( "NASDAQ: BOOK: %+v", acBook )
}

// func AddFewExtraSymbols( acPicks *[]_hit ) {
  
// }