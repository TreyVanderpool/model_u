package main

import (
	// "container/list"
	"flag"
	"time"

	// "time"

	odb "github.com/TreyVanderpool/oliver-golib/db"
	oinit "github.com/TreyVanderpool/oliver-golib/init"
	olist "github.com/TreyVanderpool/oliver-golib/list"
	ol "github.com/TreyVanderpool/oliver-golib/logging"
	osch "github.com/TreyVanderpool/oliver-golib/schwab"
	osql "github.com/TreyVanderpool/oliver-golib/sql"
	ou "github.com/TreyVanderpool/oliver-golib/utils"

	f "funcs"
)

const (
  END_OF_DAY_TIME            string = "16:00:00"
)

var (
  Log                   ol.ILogger
  DB                    *odb.DB
  SQLs                  osql.SQLs
  Schwab                *osch.SCHWAB
  gcSymbolMap           map[string]_book = make( map[string]_book )
  gcEquityMap           map[string]f.EquityEvent = make( map[string]f.EquityEvent )
  gcEquityTracker       *osch.EquityTracker = osch.NewEquityTracker()
)

type _book struct {
  Ask                   *olist.SortedListAsc[float64]
  Bid                   *olist.SortedListAsc[float64]
}

type _price struct {
  Marker                map[string]int
  TotalSize             int
  Price                 float64
}

//--------------------------------------------------
// Function: main
//--------------------------------------------------
func main() {
  lsDBName := flag.String( "db", "stocks_test", "database to use" )
	lsSymbol := flag.String( "s", "", "symbols to run with" )
  lsSymbolListName := flag.String( "sln", "", "symbols to process name" )
  lsSimDate := flag.String( "simdate", "", "date to run for" )
  lsLogLvl := flag.String( "lvl", "info", "logging level" )
  lsHostName := flag.String( "host", "olivertech.site", "stream host name" )
  lsEndOfDayTime := flag.String( "eod", END_OF_DAY_TIME, "end of day time" )
  flag.Parse()

  Log = oinit.Init( oinit.INIT_LOG, lsLogLvl ).(ol.ILogger)
  DB = oinit.Init( oinit.INIT_DB, Log, lsDBName ).(*odb.DB)
  SQLs = oinit.Init( oinit.INIT_SQLS, Log, DB ).(osql.SQLs)
  Schwab = oinit.Init( oinit.INIT_SCHWAB, Log, DB ).(*osch.SCHWAB)
  Log.SetPatterns( "%M\n", "%D %-5L %T:%F:%# %M\n" )

  Log.Info( "Starting: Symbols: %s  Date: %s  DB: %s", *lsSymbol, *lsSimDate, *lsDBName )
  defer Log.Info( "Exiting program..." )

  if *lsSymbol == "" && *lsSymbolListName == "" {
    Log.Error( "Symbol or SymbolListName is required." )
    return
  }

  lsSymbolList, _, _ := SQLs.S_SymbolsToProcessSymbolOrName( *lsSymbol, *lsSymbolListName, "" )

  for _, lSym := range lsSymbolList {
    lcBook := _book{}
    lcBook.Ask = olist.NewSortedListAsc[float64]()
    lcBook.Bid = olist.NewSortedListAsc[float64]()
    gcSymbolMap[lSym] = lcBook
  }

  // lsData := `{"data":{"1":1774275475848,"2":[{"0":92.62,"1":829,"2":8,"3":[{"0":"iexg","1":230,"2":37069487},{"0":"nyse","1":150,"2":37069169},{"0":"arcx","1":130,"2":37069488},{"0":"NSDQ","1":129,"2":37069318},{"0":"memx","1":90,"2":37069484},{"0":"batx","1":60,"2":37069483},{"0":"miax","1":30,"2":37069484},{"0":"baty","1":10,"2":37075813}]},{"0":92.61,"1":100,"2":1,"3":[{"0":"edgx","1":100,"2":37072536}]},{"0":92.59,"1":100,"2":1,"3":[{"0":"edga","1":100,"2":37069139}]},{"0":92.48,"1":200,"2":1,"3":[{"0":"mwse","1":200,"2":37069501}]},{"0":92.39,"1":10,"2":1,"3":[{"0":"ETMM","1":10,"2":36943016}]},{"0":92.29,"1":100,"2":1,"3":[{"0":"cinn","1":100,"2":37066734}]},{"0":91.8,"1":360,"2":1,"3":[{"0":"bosx","1":360,"2":37074885}]},{"0":91.79,"1":10,"2":1,"3":[{"0":"JPMS","1":10,"2":37069169}]}],"3":[{"0":92.63,"1":809,"2":6,"3":[{"0":"arcx","1":360,"2":37071772},{"0":"edgx","1":230,"2":37075080},{"0":"nyse","1":120,"2":37074739},{"0":"NSDQ","1":59,"2":37071782},{"0":"batx","1":30,"2":37069570},{"0":"iexg","1":10,"2":37075693}]},{"0":92.64,"1":20,"2":1,"3":[{"0":"memx","1":20,"2":37069169}]},{"0":92.68,"1":100,"2":1,"3":[{"0":"miax","1":100,"2":37072452}]},{"0":92.7,"1":100,"2":1,"3":[{"0":"edga","1":100,"2":37069166}]},{"0":92.77,"1":10,"2":1,"3":[{"0":"ETMM","1":10,"2":36852562}]},{"0":92.8,"1":200,"2":1,"3":[{"0":"mwse","1":200,"2":37072471}]},{"0":93.46,"1":10,"2":1,"3":[{"0":"JPMS","1":10,"2":37069520}]},{"0":93.6,"1":300,"2":1,"3":[{"0":"G","1":300,"2":37072510}]},{"0":94,"1":80,"2":2,"3":[{"0":"baty","1":50,"2":37069139},{"0":"amex","1":30,"2":37072481}]}],"key":"NFLX","ts":1774275476001},"cmd":"BQ   "}`
  // lbFile, err := os.ReadFile( "\\temp\\dbg.txt" )
  // if err != nil {
  //   Log.Exception( err )
  //   return
  // }
  // lcStreamClient := osch.NewStreamClient()
  // lcStreamClient.L = Log
  // lcStreamClient.DB = DB
  // for _, lLine := range strings.Split( string(lbFile), "\n" ) {
  //   lcBook, _ := osch.ParseBook( lLine )
  //   lcStreamClient.SaveBookToDB( lcBook, "q" )
  // }

  // os.Exit(0)

  if *lsSimDate > "" {
    f.SimulateData( *lsSymbol, *lsSimDate, Log, DB, _Equity, _Book2 )
  } else {
    go f.StartEquityStreaming( lsSymbolList, Log, DB, Schwab, *lsHostName, *lsEndOfDayTime )
    go f.StartBookStreaming( lsSymbolList, Log, DB, Schwab, *lsHostName, "nyse", *lsEndOfDayTime )
    go f.StartBookStreaming( lsSymbolList, Log, DB, Schwab, *lsHostName, "nasdaq", *lsEndOfDayTime )
  }

  lcTimeUntil := ou.GetDurationFromTime( *lsEndOfDayTime, time.Duration( 1 * time.Minute ) )

  Log.Info( "EOD: Block until: %s:  Curr Time: %s  Wait Time: %s",
            *lsEndOfDayTime, time.Now().Local().Format( ou.HH_MM_SS ), lcTimeUntil.String() )
  time.Sleep( lcTimeUntil )
}

//--------------------------------------------------
// Function: _Equity
//--------------------------------------------------
func _Equity( acData f.EquityEvent ) {
  lcEquity := f.EquityEvent{ TranDate: acData.TranDate,
                             Symbol: acData.Symbol,
                             AskSize: acData.AskSize,
                             AskPrice: acData.AskPrice,
                             BidSize: acData.BidSize,
                             BidPrice: acData.BidPrice }

  lcPrevEquity, lbFnd := gcEquityMap[acData.Symbol]

  if lbFnd {
    if lcEquity.AskSize == 0 { lcEquity.AskSize = lcPrevEquity.AskSize }
    if lcEquity.AskPrice == 0 { lcEquity.AskPrice = lcPrevEquity.AskPrice }
    if lcEquity.BidSize == 0 { lcEquity.BidSize = lcPrevEquity.BidSize }
    if lcEquity.BidPrice == 0 { lcEquity.BidPrice = lcPrevEquity.BidPrice }
  }

  lcEquity.AskBidSpread = ou.PctChg( lcEquity.AskPrice, lcEquity.BidPrice )

  gcEquityMap[acData.Symbol] = lcEquity

  Log.Debug( "EQUITY: %s : %-6s : Ask: %5d : %7.2f  Bid: %5d : %7.2f  Pct: %5.2f",
            lcEquity.TranDate,
            lcEquity.Symbol,
            lcEquity.AskSize,
            lcEquity.AskPrice,
            lcEquity.BidSize,
            lcEquity.BidPrice,
            lcEquity.AskBidSpread )
}

//--------------------------------------------------
// Function: _Book
//--------------------------------------------------
func _Book2( acData *osch.SCRBook ) {
  // Log.Info( "%-6s : %s", acData.Data.Symbol, acData.Data.MarketTimeStr )

  lcAskChgs, lcBidChgs := gcEquityTracker.AddBook( acData, true )

  Log.Info( "ASK:  New: %2d  Chg: %2d  Del: %2d  UChg: %2d : %6d   BID:  New: %2d  Chg: %2d  Del: %2d  UChg: %2d : %6d",
            len( lcAskChgs.New ),
            len( lcAskChgs.Changed ),
            len( lcAskChgs.Deleted ),
            len( lcAskChgs.Unchanged ),
            lcAskChgs.GetTotalSize(),
            len( lcBidChgs.New ),
            len( lcBidChgs.Changed ),
            len( lcBidChgs.Deleted ),
            len( lcBidChgs.Unchanged ),
            lcBidChgs.GetTotalSize() )
}

//--------------------------------------------------
// Function: _Book
//--------------------------------------------------
func _Book( acData f.BookEvent ) {

  lcEquity := gcEquityMap[acData.Symbol]

  if Log.IsDebug() {
    Log.Debug( "BOOK: %s : %-6s : %-5s : %s : %s : %5d : %7.2f  A/B/%%: %7.2f/%7.2f/%5.2f%%",
              acData.TranDate,
              acData.Symbol, 
              acData.MarkerName,
              acData.Type,
              acData.MarkerTime,
              acData.Size,
              acData.Price,
              lcEquity.AskPrice,
              lcEquity.BidPrice,
              lcEquity.AskBidSpread )
  }

  lcSymBooks := gcSymbolMap[acData.Symbol]
  lcPrice := _price{}
  var lcElem   any

  if acData.Type == "a" {
    lcElem = lcSymBooks.Ask.Get( acData.Price )
  } else {
    lcElem = lcSymBooks.Bid.Get( acData.Price )
  }

  if lcElem == nil {
    lcPrice.Marker = make( map[string]int )
    lcPrice.TotalSize = acData.Size
    lcPrice.Marker[acData.MarkerTime] = acData.Size
  } else {
    lcPrice = lcElem.(_price)
    if liMarkerSize, lbFnd := lcPrice.Marker[acData.MarkerTime]; lbFnd {
      lcPrice.TotalSize -= liMarkerSize
      lcPrice.TotalSize += acData.Size
      liSizeDiff := acData.Size - liMarkerSize
      if liSizeDiff != 0 {
        Log.Info( "SIZE: %s : %-6s [%s] : %s : %7.2f  Before: %6d  Now: %6d  Diff: %6d", 
                  acData.TranDate, acData.Symbol, acData.Type, acData.MarkerTime, acData.Price, liMarkerSize, acData.Size, liSizeDiff )
      }
    } else {
      // lcPrice := _price{}
      lcPrice.TotalSize = acData.Size
      lcPrice.Marker = make( map[string]int )
    }
    lcPrice.Marker[acData.MarkerTime] = acData.Size
  }

  lcPrice.Price = acData.Price

  if acData.Type == "a" {
    lcSymBooks.Ask.Put( acData.Price, lcPrice )
  } else {
    lcSymBooks.Bid.Put( acData.Price, lcPrice )
  }

  // lfAskList := lcSymBooks.Ask.GetFirstCount( 5 )    // Willing to SELL at these prices
  // lfBidList := lcSymBooks.Bid.GetLastCount( 5 )     // Willing to BUY at these prices
  // liTotal := 0

  // lsText := strings.Builder{}

  // fmt.Fprintf( &lsText, "ASK(SELL): %d[", len( lfAskList ) )

  // for _, lList := range lfAskList {
  //   fmt.Fprintf( &lsText, " %7.2f:%4d", lList.(_price).Price, lList.(_price).TotalSize )
  //   liTotal += lList.(_price).TotalSize
  // }

  // fmt.Fprintf( &lsText, "]%6d  BID(BUY): %d[", liTotal, len( lfBidList ) )
  // liTotal = 0

  // for i := len( lfBidList ) - 1; i >= 0; i-- {
  //   fmt.Fprintf( &lsText, " %7.2f:%4d", lfBidList[i].(_price).Price, lfBidList[i].(_price).TotalSize )
  //   liTotal += lfBidList[i].(_price).TotalSize
  // }

  // fmt.Fprintf( &lsText, "]%6d", liTotal )

  // Log.Info( "%s : %-6s %s", acData.TranDate, acData.Symbol, lsText.String() )

  // if len( lfAskList ) == 5 && len( lfBidList ) == 5 {
  //   lsText.Reset()
  // }
}