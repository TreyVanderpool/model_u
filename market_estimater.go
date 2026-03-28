package main

import (
	// "container/list"
	"flag"
	"time"

	// "time"

	odb "github.com/TreyVanderpool/oliver-golib/db"
	oinit "github.com/TreyVanderpool/oliver-golib/init"
	// olist "github.com/TreyVanderpool/oliver-golib/list"
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
  gcEquityTracker       *osch.EquityTracker = osch.NewEquityTracker()
)

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

  // for _, lSym := range lsSymbolList {
  //   lcBook := _book{}
  //   lcBook.Ask = olist.NewSortedListAsc[float64]()
  //   lcBook.Bid = olist.NewSortedListAsc[float64]()
  //   gcSymbolMap[lSym] = lcBook
  // }

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
    return
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
func _Equity( acEquity *osch.SEquityOne ) {
  lcPrevEquity := gcEquityTracker.GetEquityVersion( acEquity.Symbol, 0 )

  if lcPrevEquity != nil {
    if acEquity.AskSize == 0 { acEquity.AskSize = lcPrevEquity.AskSize }
    if acEquity.AskPrice == 0 { acEquity.AskPrice = lcPrevEquity.AskPrice }
    if acEquity.BidSize == 0 { acEquity.BidSize = lcPrevEquity.BidSize }
    if acEquity.BidPrice == 0 { acEquity.BidPrice = lcPrevEquity.BidPrice }
  }

  gcEquityTracker.AddEquity( acEquity )
  lfPricePctChg := ou.PctChg( acEquity.AskPrice, acEquity.BidPrice )

  Log.Debug( "EQUITY: %s : %-6s : Ask: %5d : %7.2f  Bid: %5d : %7.2f  Pct: %5.2f",
            acEquity.TranDate,
            acEquity.Symbol,
            acEquity.AskSize,
            acEquity.AskPrice,
            acEquity.BidSize,
            acEquity.BidPrice,
            lfPricePctChg )
}

//--------------------------------------------------
// Function: _Book
//--------------------------------------------------
func _Book2( acData *osch.SCRBook ) {
  liAskPrice := 0.0
  liAskVol := 0
  liBidPrice := 0.0
  liBidVol := 0

  lcAskChgs, lcBidChgs := gcEquityTracker.AddBook( acData, true )

  if lcAskChgs == nil || lcBidChgs == nil { return }

  lcCurrEquity := gcEquityTracker.GetEquityVersion( acData.Data.Symbol, 0 )

  if lcCurrEquity != nil {
    liAskPrice = lcCurrEquity.AskPrice
    liAskVol = lcCurrEquity.AskSize
    liBidPrice = lcCurrEquity.BidPrice
    liBidVol = lcCurrEquity.BidSize
  }

  liOFI := lcBidChgs.DeltaSize - lcAskChgs.DeltaSize

  Log.Info( "BOOK: %s : ASK:  N/C/D/U: %2d %2d %2d %2d : %6d   BID:  N/C/D/U: %2d %2d %2d %2d : %6d : OBI: %5.2f : OFI: %5d   EQ Ask: %7.2f : %5d  Bid: %7.2f : %5d",
            acData.Data.MarketTimeStr,
            len( lcAskChgs.New ),
            len( lcAskChgs.Changed ),
            len( lcAskChgs.Deleted ),
            len( lcAskChgs.Unchanged ),
            lcAskChgs.GetTotalSize(),
            len( lcBidChgs.New ),
            len( lcBidChgs.Changed ),
            len( lcBidChgs.Deleted ),
            len( lcBidChgs.Unchanged ),
            lcBidChgs.GetTotalSize(),
            acData.GetOBI(),
            liOFI,
            liAskPrice, liAskVol,
            liBidPrice, liBidVol )
}
