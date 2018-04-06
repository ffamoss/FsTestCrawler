namespace Crawler

module CrawlerTests =

    open Xunit
    open System
    open System.IO
    open Core
    
       
    [<Fact>]
    let ``Smoke Test`` () =        
        let tmpUri = new Uri(@"http://news.google.com")
        
        let tmpTask = { 
            BaseUri = tmpUri; 
            ReplaceUrlToLocal = true;
            CrawlDepth = 1;
            LocalPath = @"C:\tmp\crawler";
            IgnoreOtherDomains = false
        }
                          
        Directory.CreateDirectory(tmpTask.LocalPath) |> ignore

        Core.startCrawlerTask tmpTask fetchDefault storeDataDefault
    
