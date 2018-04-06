
namespace Crawler

open System
open System.Collections.Concurrent
open System.IO
open System.Net
open System.Text.RegularExpressions
open HtmlAgilityPack
open System.Text

module Infra =         

    type Log = 
        | Info of string
        | Warning of string
        | Error of string
        | Exception of string * Exception

    let formatlog msg = 
        let formater lType str = 
            sprintf "Thread: %i    %s    %s" Threading.Thread.CurrentThread.ManagedThreadId lType str
        match msg with
        | Info x -> formater "info" x
        | Warning x -> formater "wrn" x
        | Error x -> formater "err" x
        | Exception (x, ex) -> formater "exception" (sprintf "%s\n%s" x (ex.ToString()))
 
    let log msg =
        //Note change to real logger
        printfn "%s" (formatlog msg)

    let logError str =
        log (Error str)

    let logInfo str =
        log (Info str)

    type HttpContent = 
        | Html of Uri*string
        | Css of Uri*string
        | Png of Uri*byte array
        | Jpg of Uri*byte array
        | Other of Uri*string*byte array

    let fetchContent (resp: HttpWebResponse) url=
        let checkContentType pattern = 
            Regex(pattern).IsMatch(resp.ContentType)

        let getData() = 
            use stream = resp.GetResponseStream()
            use memStream = new MemoryStream()
            stream.CopyTo(memStream)
            memStream.ToArray()

        let getEncoding() =
            try
                if String.IsNullOrWhiteSpace resp.ContentEncoding then
                    Encoding.UTF8
                else
                    Encoding.GetEncoding(resp.ContentEncoding)
             with
             | _ -> Encoding.UTF8

        let getStringData() =
            getData() |>  getEncoding().GetString            

        match resp.ContentType with
        | _ when checkContentType "html" -> Html (url, getStringData())
        | _ when checkContentType "css" -> Css (url, getStringData())
        | _ when checkContentType "jpg" || checkContentType "jpeg" -> Jpg (url, getData())
        | _ when checkContentType "png" -> Png (url, getData())
        | _ ->
            logInfo (sprintf "Ignore ContentType [%s]" resp.ContentType)
            Other (url, resp.ContentType, getData())

    let fetchWebPage (applyRequestSettings : HttpWebRequest -> HttpWebRequest) (url : Uri) =
        try
            logInfo (sprintf "Start fetch url [%s]" url.AbsoluteUri)

            let req = applyRequestSettings (WebRequest.Create(url) :?> HttpWebRequest)          
            use resp = req.GetResponse()
           
            Some (fetchContent (resp :?> HttpWebResponse) url)
        with 
        | ex -> 
            log (Log.Exception ((sprintf "Error while fetch [%s]" url.AbsoluteUri), ex))
            None   

    let htmlNodeProcessor transformAttribute (uriBase: Uri) (node: HtmlAttribute) : Uri option =
        match Uri.TryCreate(uriBase, node.Value) with
        | true,  url -> 
            if url.IsFile then None
            else
                transformAttribute url node
                Some url
        | _ -> 
            logInfo (sprintf "Can't crate absolute uri for [%s]" node.Value)
            None

    let extactAndprocessLinks processor (uriBase: Uri) html =
       let attrProcessor (n: HtmlNode) =
           [
               for attrName in ["background"; "href"; "src"; "lowsrc"] do
                   let attr = n.Attributes.[attrName]
                   if attr <> null && not (attrName = "href" && n.Name = "link") then
                       yield processor uriBase attr
           ] |> List.choose id
    
       let hrefProcessor (n: HtmlNode) =
           processor uriBase n.Attributes.["href"]
              
       let doc = HtmlDocument()
       doc.LoadHtml(html)
    
       let attr = doc.DocumentNode.SelectNodes("//*[@background or @lowsrc or @src or @href]");       
       let attrLinks =  match attr with
                        |null -> []
                        |_ ->  attr |> Seq.map attrProcessor |> Seq.toList |> List.collect id
           
       let hrefs = doc.DocumentNode.SelectNodes("//a[@href]");
       let hrefsLinks = match hrefs with
                        | null -> []
                        |_ ->  hrefs |> Seq.choose hrefProcessor |> Seq.toList
       
       let links = attrLinks @ hrefsLinks |> List.distinct

       (doc.DocumentNode.OuterHtml, links)

    
    let fileNameGenerator (starUri:Uri) (uri: Uri) =
        let generateHashName() =
            sprintf "%i%s" (uri.AbsoluteUri.GetHashCode()) ".html" 

        let isNone str = String.IsNullOrWhiteSpace(str)

        let invalidCharList = [ for c in Path.GetInvalidFileNameChars() do
                                    yield c.ToString()
                                for c in Path.GetInvalidPathChars() do
                                    yield c.ToString()]
        let replace (str:string) (c: string) =
            str.Replace(c,"")
                    
        let path = invalidCharList |> List.fold replace uri.LocalPath               

        match uri with
        | _ when uri = starUri -> "index.html"
        | _ when uri.IsFile -> Path.GetFileName(uri.LocalPath)
        | _ when isNone path -> generateHashName()
        | _ -> 
            let maxLenght = 100
            let path = match path.Length with
                        | l when l > maxLenght -> path.Substring(l - maxLenght)
                        | _ -> path
            let fi = new FileInfo(path)
            match fi with
            | _ when isNone fi.Name || isNone fi.Extension -> generateHashName()
            | _ -> fi.Name
    
module Core =
    open Infra    
    open System.Threading

    type Settings()= 
        let defaultProxy = WebRequest.GetSystemWebProxy()
        do defaultProxy.Credentials <- CredentialCache.DefaultNetworkCredentials

        member val MaxParallelDownloadsCount = 10 with get, set
        member val WorkersLimit = 5 with get, set
        member val RequestTimeout = TimeSpan.FromMinutes(1.0) with get, set
        member val IdleTimeout = TimeSpan.FromSeconds(15.0) with get, set
        member val RetryCount = 3 with get, set
        member val UserAgent ="Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)" with get, set
        member val Proxy = defaultProxy with get, set


    type CrawlerTask = {
        BaseUri:Uri;
        LocalPath:string;
        CrawlDepth:int;
        IgnoreOtherDomains:bool;
        ReplaceUrlToLocal:bool;
        }

    type DownloaderMsg =
            | FetchUrl of  Uri*int*int
            | AgentFetchUrl of MailboxProcessor<DownloaderMsg>*Uri*int*int
            | DownloadedComplete of MailboxProcessor<DownloaderMsg>*HttpContent*int
            | DownloadedFail of MailboxProcessor<DownloaderMsg>*Uri*int*int
            | AgentStop
            | Stop

    type WorkerMsg =
            | Stop
            | Process of CrawlerTask*HttpContent*int
            | AgentProcessComplete of MailboxProcessor<WorkerMsg>
            | AgentProcess of MailboxProcessor<WorkerMsg>*CrawlerTask*HttpContent*int
            | AgentStop

    type SystemMsg =
            | Stop
            | CrawlerTask of CrawlerTask
            | NeedDownload of  Uri*int
            | DownloadedComplete of HttpContent*int
            | DownloaderReady of MailboxProcessor<DownloaderMsg>
            | WorkerReady of MailboxProcessor<WorkerMsg>
            | NoActiveWorkers
            | NowActiveDownloaders

    type SystemState =
        | Working
        | DownloadingStageComplete
        | ProcessingStageComplete
        | AllStagesComplete

    let defaultSettings = new Settings()
    
    let applyRequestSettings (settings: Settings) (req: HttpWebRequest) : HttpWebRequest=
          req.UserAgent <- settings.UserAgent
          req.Timeout <- (int)settings.RequestTimeout.TotalMilliseconds         
          req.Proxy <- settings.Proxy
          req

    let applyDefaultRequestSettings req =
        applyRequestSettings defaultSettings req

   
    let fetchDefault url = 
        fetchWebPage applyDefaultRequestSettings url
    
    let storeDataDefault path data =
             File.WriteAllBytes(path, data)

    let startCrawlerTask (cTask: CrawlerTask) fetchUrl storeData =

        let waitEndEvent = new AutoResetEvent(false)                            
        let linksDataCache = new ConcurrentDictionary<Uri,int>()
        
        let systemSupervisor =
             MailboxProcessor.Start(fun x ->
                let rec loop systemState
                             task 
                             (downloader: MailboxProcessor<DownloaderMsg> option) 
                             (worker: MailboxProcessor<WorkerMsg> option) 
                             =
                    let stopSystem() =
                          downloader.Value.Post <| DownloaderMsg.Stop
                          worker.Value.Post <| WorkerMsg.Stop
                          x.Post <| SystemMsg.Stop

                          waitEndEvent.Set() |> ignore

                    async {
                        if downloader.IsNone || worker.IsNone then
                             Async.RunSynchronously(
                                x.Scan(fun m ->                          
                                           match m with
                                           | SystemMsg.DownloaderReady dwn -> 
                                               let runLoop =
                                                   async{ return! loop systemState task (Some dwn) worker }
                                               Some(runLoop)
                                           | SystemMsg.WorkerReady wrk ->
                                                let runLoop =
                                                    async{ return! loop systemState task downloader (Some wrk)}
                                                Some(runLoop)
                                           | _ -> None))
                            
                        let! msg = x.Receive()

                        match msg with
                        | SystemMsg.CrawlerTask t ->
                            downloader.Value.Post <| DownloaderMsg.FetchUrl(t.BaseUri, 0, 0)
                            return! loop SystemState.Working (Some t) downloader worker
                        
                        | SystemMsg.NeedDownload(uri,level) ->
                            downloader.Value.Post <| DownloaderMsg.FetchUrl(uri, 0, level)
                            return! loop SystemState.Working task downloader worker
                        
                        | SystemMsg.DownloadedComplete(content, level) ->                         
                            worker.Value.Post <| WorkerMsg.Process(task.Value, content, level)
                            return! loop SystemState.Working task downloader worker
                        
                        | SystemMsg.NoActiveWorkers ->
                            match systemState with
                            | SystemState.Working -> 
                                return! loop SystemState.ProcessingStageComplete task downloader worker
                            | SystemState.DownloadingStageComplete ->                             
                                stopSystem()
                                return! loop SystemState.AllStagesComplete task downloader worker
                            | SystemState.ProcessingStageComplete ->
                                return! loop systemState task downloader worker
                            | SystemState.AllStagesComplete ->
                                return! loop systemState task downloader worker
                        
                        | SystemMsg.NowActiveDownloaders ->
                            match systemState with
                            | SystemState.Working -> 
                                return! loop SystemState.DownloadingStageComplete task downloader worker
                            | SystemState.DownloadingStageComplete ->                             
                                return! loop systemState task downloader worker
                            | SystemState.ProcessingStageComplete ->
                                stopSystem()
                                return! loop SystemState.AllStagesComplete task downloader worker
                            | SystemState.AllStagesComplete ->
                                return! loop systemState task downloader worker

                        | SystemMsg.Stop ->
                            logInfo (sprintf "systemSupervisor stopped")
                            (x :> IDisposable).Dispose()
                        | some -> logError (sprintf "Unexpected msg %A for systemSupervisor" some)
                    }
                loop SystemState.Working None None None)

        let workerAgent (system: MailboxProcessor<SystemMsg>) id =

            let replaceUriInAttributeToLocal startUri localPath uri (node: HtmlAttribute) =
                let filePath = Path.Combine(localPath, fileNameGenerator startUri uri)
                node.Value <- filePath
    
            let taskhtmlNodeProcessor task = 
                if task.ReplaceUrlToLocal then
                    htmlNodeProcessor (replaceUriInAttributeToLocal task.BaseUri task.LocalPath)
                else
                    htmlNodeProcessor (fun _ _ -> ())
    
            let storeUrlData startUri localPath url data =
                 let filePath = Path.Combine(localPath, fileNameGenerator startUri url)
                 logInfo (sprintf "Store [%s] to [%s]" url.AbsolutePath filePath)
    
                 storeData filePath data

            MailboxProcessor.Start(fun x->       
                let rec loop() =
                    async {
                        let! msg = x.Receive()
                        match msg with
                        | AgentProcess(supervisor, t, content, level)->
                            let processContent url data =
                                linksDataCache.TryAdd(url, 0) |> ignore
                                                                
                                storeUrlData t.BaseUri t.LocalPath url data
                               
                            match content with
                            | HttpContent.Html(url, htmlData) -> 
                                linksDataCache.TryAdd(url, 0) |> ignore
                                                                
                                let linkFilter url =
                                    match url with
                                    |_ when linksDataCache.ContainsKey(url) -> false
                                    |_ when t.IgnoreOtherDomains && t.BaseUri.Host <> url.Host -> false
                                    |_ when url.IsFile -> false
                                    |_ -> true                                            

                                let newHtml, links = extactAndprocessLinks  (taskhtmlNodeProcessor t) url htmlData

                                storeUrlData t.BaseUri t.LocalPath url (Encoding.UTF8.GetBytes(newHtml))

                                if level < t.CrawlDepth then
                                    links 
                                    |> List.filter linkFilter
                                    |> List.iter (fun u -> system.Post <| SystemMsg.NeedDownload(u, level+1) )
                                
                            | HttpContent.Css(url,data)->
                                processContent url (Encoding.UTF8.GetBytes(data))
                            | HttpContent.Other(url,_,data)->
                                processContent url data
                            | HttpContent.Jpg(url,data)->
                                processContent url data
                            | HttpContent.Png(url,data)->
                                processContent url data    
                            
                            supervisor.Post <| WorkerMsg.AgentProcessComplete(x)
                            return! loop()
                        | AgentStop ->
                            logInfo (sprintf "workerAgent id %i stopped" id)
                            (x :> IDisposable).Dispose()
                        | some -> logError (sprintf "Unexpected msg %A workerAgent id %i " some id)
                        }

                loop())

        let workersSupervisor (system: MailboxProcessor<SystemMsg>) =
             MailboxProcessor.Start(fun x->
                let workerAgents = [
                    for i in [0..defaultSettings.WorkersLimit] do
                        yield workerAgent system i]              

                let freeAgents = new ConcurrentQueue<MailboxProcessor<WorkerMsg>>(workerAgents)              

                let rec loop() =
                    async {
                        let! msgOpt = x.TryReceive ((int)defaultSettings.IdleTimeout.TotalMilliseconds)
                        match msgOpt with
                        | None ->
                            logInfo (sprintf "workersSupervisor receive timeout %s" (defaultSettings.IdleTimeout.ToString()))
                            if freeAgents.Count = workerAgents.Length then
                                system.Post <| SystemMsg.NoActiveWorkers
                            
                            return! loop()
                        | Some msg ->
                            match msg with
                            | WorkerMsg.Process (task, content, level) ->
                                match freeAgents.TryDequeue() with
                                | true, agent -> 
                                    agent.Post <| WorkerMsg.AgentProcess(x, task, content, level)
                                //Note think about use x.Scan to process this message only when has any free agents
                                | _ -> Async.StartAsTask(
                                            async {
                                                    do! Async.Sleep(1000) 
                                                    x.Post <| WorkerMsg.Process (task, content, level)
                                             }) |> ignore
                                return! loop()
                            
                            | WorkerMsg.AgentProcessComplete agent ->
                                freeAgents.Enqueue agent
                                return! loop()
                            
                            | WorkerMsg.Stop ->
                                logInfo (sprintf "workersSupervisor stopped with all workers")
                                workerAgents |> List.iter (fun a -> a.Post <| WorkerMsg.AgentStop)
                                (x :> IDisposable).Dispose()
                            | some -> logError (sprintf "Unexpected msg %A for workersSupervisor" some)
                        }
                system.Post <| SystemMsg.WorkerReady x
                loop())


        let downloadAgent id =
            MailboxProcessor.Start(fun x->       
                let rec loop() =
                    async {
                        let! msg = x.Receive()
                        match msg with
                        | DownloaderMsg.AgentFetchUrl (supervisor, url, attempt, level) ->
                            match fetchUrl url with
                            | Some data ->
                                logInfo (sprintf "downloadAgent %i complete [%s]" id url.AbsoluteUri)
                                supervisor.Post <| DownloaderMsg.DownloadedComplete(x, data, level)
                            | _ ->  
                                logInfo (sprintf "downloadAgent %i failed [%s]" id url.AbsoluteUri)
                                supervisor.Post <| DownloaderMsg.DownloadedFail(x, url, attempt, level)
                            return! loop()
                        | DownloaderMsg.AgentStop -> 
                            logInfo (sprintf "downloadAgent id %i stopped" id)
                            (x :> IDisposable).Dispose()
                        | some -> logError (sprintf "Unexpected msg %A downloadAgent id %i " some id)
                        }
                loop())


        let downloadSupervisor (system: MailboxProcessor<SystemMsg>) = 
            MailboxProcessor.Start(fun x->
                let downloadAgents = [
                    for i in [0..defaultSettings.MaxParallelDownloadsCount] do
                        yield downloadAgent i]              

                let freeAgents = new ConcurrentQueue<MailboxProcessor<DownloaderMsg>>(downloadAgents)              

                let rec loop() =
                    async {
                        let! msgOpt = x.TryReceive ((int)defaultSettings.IdleTimeout.TotalMilliseconds)
                        match msgOpt with
                        | None ->
                            logInfo (sprintf "downloadSupervisor receive timeout %s" (defaultSettings.IdleTimeout.ToString()))
                            if freeAgents.Count = downloadAgents.Length then
                                system.Post <| SystemMsg.NowActiveDownloaders
                            
                            return! loop()
                        | Some msg ->
                            match msg with
                            | DownloaderMsg.FetchUrl (url, attempt, level) -> 
                                match freeAgents.TryDequeue() with
                                | true, agent -> agent.Post <| DownloaderMsg.AgentFetchUrl (x, url, attempt, level)
                                | _ -> Async.StartAsTask(
                                            async {
                                                    do! Async.Sleep(1000) 
                                                    x.Post <| DownloaderMsg.FetchUrl (url, attempt, level)
                                                }) |> ignore
                                
                                return! loop()
                                
                            | DownloaderMsg.DownloadedFail(agent, url, tryCount, level) -> 
                                freeAgents.Enqueue agent
                                
                                if tryCount >= defaultSettings.RetryCount then
                                    logError (sprintf "Reach max retry count (%i) for Uri %s" defaultSettings.RetryCount url.AbsoluteUri) 
                                else
                                    x.Post <|  DownloaderMsg.FetchUrl(url, tryCount + 1, level)
                                
                                return! loop()
                                
                             | DownloaderMsg.DownloadedComplete(agent, content, level) ->
                                freeAgents.Enqueue agent

                                system.Post <| SystemMsg.DownloadedComplete(content, level)

                                return! loop()
                            
                             | DownloaderMsg.Stop -> 
                                downloadAgents |> List.iter (fun a -> a.Post <|  DownloaderMsg.AgentStop)
                                (x :> IDisposable).Dispose()

                             | some -> logError (sprintf "Unexpected msg %A for downloadSupervisor" some)
                        }
                
                system.Post <| SystemMsg.DownloaderReady x
                loop())

        let system = systemSupervisor 
        downloadSupervisor system |> ignore
        workersSupervisor system |> ignore

        system.Post <| SystemMsg.CrawlerTask cTask

        logInfo "Init complete"
        waitEndEvent.WaitOne() |> ignore
       
        logInfo (sprintf "Work done. %i links processed" linksDataCache.Count)