package main

import (
	"fmt"
	"log"
	"encoding/json"
	"strings"
	"strconv"
	"time"
	"math"
	"github.com/gocolly/colly"
	"github.com/PuerkitoBio/goquery"
	"net/http"
	"os"
	"bufio"
)

func main(){
	fmt.Println("Loading config")
	config := loadConfiguration("config.json")
	workspace:="workspace"
	if _, err := os.Stat(workspace); os.IsNotExist(err) {
		os.Mkdir(workspace, os.ModePerm)
	}
	path:="workspace/"+config.VendorId
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, os.ModePerm)
	}
	f, err := os.OpenFile(path+"/logs", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)
	fmt.Println("Preparing list ID...")
	list(path, config)
	fmt.Println("List ID OK")
	fmt.Println("Start Scrapping")
	log.Println("Start Scrapping")
	scrapping(path, config)
	fmt.Println("Scrapping Finish!")
	log.Println("Scrapping Finish!")
}

func list(path string, config Config){
	f :=path+"/tot"
	ff :=path+"/fin"
	total:=0
	length:=500
	start:=0
	s :=[]string{}
	if !fileExists(f) {
		total = totalid(config)
		if total==0{
			log.Fatalf("No total records!")
		}
		s = append(s,strconv.Itoa(total))
		for i:=start;i<total;i=i+length{
			s = append(s,strconv.Itoa(i))
		}
		writef(f, s)
		writef(ff, nil)
    } else {
		tot,_ := readLines(f)
		ltot := len(tot)
		if ltot==0{
			total = totalid(config)
			if total==0{
				log.Fatalf("No total records!")
			}
			s = append(s,strconv.Itoa(total))
			for i:=start;i<total;i=i+length{
				s = append(s,strconv.Itoa(i))
			}
			writef(f, s)
			writef(ff, nil)
		}else{
			total,_ = strconv.Atoi(tot[0]);
		}
    }
	time.Sleep(5 * time.Second)
	for i:=start;i<total;i=i+length{
		p :=path+"/"+strconv.Itoa(i)
		if !fileExists(p) {
			list:=getlist(config, strconv.Itoa(i), strconv.Itoa(length))
			writef(p, list)
			time.Sleep(10 * time.Second)
		}else{
			a, _ := readLines(p)
			if len(a)<length{
				list:=getlist(config, strconv.Itoa(i), strconv.Itoa(length))
				writef(p, list)
				time.Sleep(10 * time.Second)
			}
		}
	}
}

func scrapping(path string, config Config){
	tot,_ := readLines(path+"/tot")
	fin,_ := readLines(path+"/fin")
	finid :=map[string]string{}
	//ubah fin jadi map biar gampang mau cari
	for i:=0;i<len(fin);i++{
		finid[fin[i]]=fin[i]
	}
	ltot := len(tot)
	if ltot==0{
		log.Fatal("Error total")
	}
	// total:=tot[0]
	for i:=1;i<ltot;i++{
		fname :=tot[i]
		//read file id
		listid,_ := readLines(path+"/"+fname)
		for j:=0;j<len(listid);j++{
			id :=listid[j]
			if _, found := finid[id]; !found {
				if scrapDetail(config, id){
					time.Sleep(20*time.Second)
					writeid(path+"/fin", []string{id})
				}
			}
		}
	}
}

func readLines(path string) ([]string, error) {
    file, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    var lines []string
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        lines = append(lines, scanner.Text())
    }
    return lines, scanner.Err()
}

func writef(path string, data []string){
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	
	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}
	datawriter := bufio.NewWriter(file)
	for _, data := range data {
		_, _ = datawriter.WriteString(data + "\n")
	}
	datawriter.Flush()
	file.Close()
}
func writeid(path string, data []string){
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	
	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}
	datawriter := bufio.NewWriter(file)
	for _, data := range data {
		_, _ = datawriter.WriteString(data + "\n")
	}
	datawriter.Flush()
	file.Close()
}
func fileExists(filename string) bool {
    info, err := os.Stat(filename)
    if os.IsNotExist(err) {
        return false
    }
    return !info.IsDir()
}

type eproc struct{
	VendorId				int		`json:"vendor_id,omitempty"`
	VendorName				string	`json:"vendor_name,omitempty"`
	VendorType				int		`json:"vendor_type,omitempty"`
	PackageName			string	`json:"package_name,omitempty"`
	Skpd					string	`json:"skpd,omitempty"`
	ShortPagu				string	`json:"short_pagu,omitempty"`
	LongPagu				string	`json:"long_pagu,omitempty"`
	NumberPagu				float64	`json:"number_pagu,omitempty"`
	Hps						string	`json:"hps,omitempty"`
	ShortHps				string	`json:"short_hps,omitempty"`
	NumberHps				float64	`json:"number_hps,omitempty"`
	Participant				string	`json:"participant,omitempty"`
	StartDate				string	`json:"start_date,omitempty"`
	EndDate				string	`json:"end_date,omitempty"`
	RangeDate				string	`json:"range_date,omitempty"`
	UrlDetail				string	`json:"url_detail,omitempty"`
	Step					string	`json:"step,omitempty"`
	Agency					string	`json:"agency,omitempty"`
	Satker					string	`json:"satker,omitempty"`
	Category				string	`json:"category,omitempty"`
	EprocMethod			string	`json:"eproc_method,omitempty"`
	QualificationMethod	string	`json:"qualification_method,omitempty"`
	DocumentMethod			string	`json:"document_method,omitempty"`
	EvaluationMethod		string	`json:"evaluation_method,omitempty"`
	YearBudget				string	`json:"year_budget,omitempty"`
	Location				string	`json:"location,omitempty"`
	QualificationTerms		string	`json:"qualification_terms,omitempty"`
	BotUpdate				string	`json:"bot_update,omitempty"`
	CompanyName			string	`json:"company_name,omitempty"`
	CompanyNpwp			string	`json:"company_npwp,omitempty"`
	CompanyAddress			string	`json:"company_address,omitempty"`
	BidPrice				float64	`json:"bid_price,omitempty"`
	CorrectedPrice			float64	`json:"corrected_price,omitempty"`
	NegotiationsPrice		float64	`json:"negotiations_price,omitempty"`
	IsFavorite				int	`json:"is_favorite,omitempty"`
	Subscribers				int	`json:"subscribers,omitempty"`
	Tags					string	`json:"tags,omitempty"`
	CreatedAt				string	`json:"created_at,omitempty"`
	UpdatedAt				string	`json:"updated_at,omitempty"`	
}


type procDt struct{
	Draw string `json:"draw,omitempty"`
	RecordsTotal int `json:"recordsTotal,omitempty"`
	RecordsFiltered int `json:"recordsFiltered,omitempty"`
	Data [][]string `json:"data,omitempty"`
}

type Config struct {
    EsUrl string `json:"es_url"`
	EsAuth string `json:"es_auth"`
	EsIndex string `json:"es_index"`
	LPSEUrl string `json:"lpse_url"`
	VendorId string `json:"vendor_id"`
	VendorName string `json:"vendor_name"`
	VendorType string `json:"vendor_type"`
}

func totalid(config Config) int{
	url:=config.LPSEUrl
	start:="0"
	length:="1"
	url_dt := url+"/dt/lelang?draw=2&columns%5B0%5D%5Bdata%5D=0&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=1&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=2&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=3&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=false&columns%5B3%5D%5Borderable%5D=false&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=4&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=0&order%5B0%5D%5Bdir%5D=desc&start="+start+"&length="+length+"&search%5Bvalue%5D=&search%5Bregex%5D=false&_=1565793389858"
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"),
	)
	c.Limit(&colly.LimitRule{
		DomainGlob:  "*httpbin.*",
		Parallelism: 2,
		RandomDelay: 20 * time.Second,
	})
	total :=0
	c.OnResponse(func(r *colly.Response) {
		if strings.Index(r.Headers.Get("Content-Type"), "json") == -1 {
			return
		}
		
		dt := &procDt{}
		
		err := json.Unmarshal(r.Body, dt)
		if err != nil {
			log.Fatal(err)
		}
		total = dt.RecordsTotal
	})
	c.OnError(func(r *colly.Response, err error) {
		log.Fatalf("Request URL:%s failed with error:%s", r.Request.URL, err)
	})
	c.Visit(url_dt)
	return total
}

func getlist(config Config, start string, length string)[]string{
	url:=config.LPSEUrl
	url_dt := url+"/dt/lelang?draw=2&columns%5B0%5D%5Bdata%5D=0&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=1&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=2&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=3&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=false&columns%5B3%5D%5Borderable%5D=false&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=4&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=0&order%5B0%5D%5Bdir%5D=desc&start="+start+"&length="+length+"&search%5Bvalue%5D=&search%5Bregex%5D=false&_=1565793389858"
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"),
		
		
	)

	c.Limit(&colly.LimitRule{
		DomainGlob:  "*httpbin.*",
		Parallelism: 2,
		RandomDelay: 60 * time.Second,
	})

	ids := make([]string, 0)
	c.OnResponse(func(r *colly.Response) {
		if strings.Index(r.Headers.Get("Content-Type"), "json") == -1 {
			return
		}
		
		dt := &procDt{}
		
		err := json.Unmarshal(r.Body, dt)
		if err != nil {
			log.Fatal(err)
		}
		var data = dt.Data
		for i := 0; i < len(data); i++ {
			ids = append(ids, data[i][0])
		}
	})
	c.OnError(func(r *colly.Response, err error) {
		log.Fatalf("Request URL:%s failed with error:%s", r.Request.URL, err)
	})
	c.OnRequest(func(r *colly.Request) {
		// log.Println("visiting", r.URL.String())
	})

	c.Visit(url_dt)
	c.Wait()
	return ids
}

func scrapDetail(config Config, id string) bool{
	fmt.Println("Grabbing data Vendor ID: ", config.VendorId, " Kode Tender: ", id)
	log.Println("Grabbing data Vendor ID: ", config.VendorId, " Kode Tender: ", id)
	eproc := detail(config, id)
	time.Sleep(5*time.Second)
	comp(&eproc, config, id)
	time.Sleep(2*time.Second)
	price(&eproc, config, id)
	fmt.Println("Grabbing is done!")
	log.Println("Grabbing is done!")
	j:=tojson(eproc)
	idDoc := config.VendorId+"-"+id
	fmt.Println("Updating data to ES with id ", idDoc)
	log.Println("Updating data to ES with id ", idDoc)
	s, m:=update(config, j, idDoc)
	if s{
		fmt.Println("Success updating data to ES with id ", idDoc)
		log.Println("Success updating data to ES with id ", idDoc)
	}else{
		log.Println("Updating data failed. ID Document: ", idDoc, " Error:", m)
	}
	return s
}
func detail(config Config, id string) eproc{
	url := config.LPSEUrl
	urlDetail := url+"/lelang/"+id+"/pengumumanlelang"
	dnow :=time.Now().Format("2006-01-02")
	now :=time.Now().Format("2006-01-02 15:04:05")
	vendorId, _ := strconv.Atoi(config.VendorId)
	vendorType, _ := strconv.Atoi(config.VendorType)
	eproc := eproc{
		VendorId: vendorId,
		VendorName: config.VendorName,
		VendorType: vendorType,
		UrlDetail: urlDetail,
		BotUpdate: dnow,
		CreatedAt: now,
	} 
	url_detail := url+"/lelang/"+id+"/pengumumanlelang"
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"),	
	)
	c.Limit(&colly.LimitRule{
		DomainGlob:  "*httpbin.*",
		Parallelism: 2,
		RandomDelay: 20 * time.Second,
	})
	c.OnHTML(".content table tbody", func(e *colly.HTMLElement) {
		goquerySelection := e.DOM
		goquerySelection.Find("tr").Each(func(_ int, s *goquery.Selection){
			field := s.Find("th:first-child").Text()
			value := s.Find("td:nth-child(2)").Text()
			value = strings.TrimSpace(value)
			if field=="Nama Tender"{
				eproc.PackageName = value
			}else  if field=="Tahap Tender Saat ini"{
				hvalue,_ := s.Find("td:nth-child(2)").Html()
				eproc.Step = hvalue
			}else  if field=="Instansi"{
				eproc.Skpd = value
				eproc.Agency = value
			}else  if field=="Satuan Kerja"{
				eproc.Satker = value
			}else  if field=="Peserta Tender"{
				eproc.Participant = value
			}else  if field=="Kategori"{
				eproc.Category = value
			}else  if field=="Sistem Pengadaan"{
				eproc.EprocMethod = value
				eproc.QualificationMethod = value
				eproc.DocumentMethod = value
				eproc.EvaluationMethod = value
			}else  if field=="Tahun Anggaran"{
				eproc.YearBudget = value
			}else  if field=="Lokasi Pekerjaan"{
				eproc.Location = value
			}else  if field=="Nilai Pagu Paket"{
				nvalue := tonum(value)
				eproc.LongPagu = value
				eproc.NumberPagu = nvalue
				eproc.ShortPagu = shortNum(nvalue)
				
				field2 := s.Find("th:nth-child(3)").Text()
				value2 := s.Find("td:nth-child(4)").Text()
				if field2=="Nilai HPS Paket"{
					nvalue2 := tonum(value2)
					eproc.Hps = value2
					eproc.NumberHps = nvalue2
					eproc.ShortHps = shortNum(nvalue2)
				}
			}else if strings.Contains(field, "Syarat Kualifikasi"){
				hvalue, _ := s.Find("td:nth-child(2)").Html()
				eproc.QualificationTerms = hvalue
			}
		})
	})
	c.OnError(func(r *colly.Response, err error) {
		log.Println("Request URL:", r.Request.URL, "failed with error:", err)
	})
	c.Visit(url_detail)
	return eproc
}

func comp(eproc *eproc, config Config, id string){
	url := config.LPSEUrl
	 
	url_detail := url+"/evaluasi/"+id+"/pemenang"

	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"),
	)

	c.Limit(&colly.LimitRule{
		DomainGlob:  "*httpbin.*",
		Parallelism: 2,
		RandomDelay: 20 * time.Second,
	})
	c.OnHTML(".content table tbody tr table tr:nth-child(2)", func(e *colly.HTMLElement) {
		goquerySelection := e.DOM
		p:=goquerySelection.Find("td:nth-child(1)").Text()
		q:=goquerySelection.Find("td:nth-child(2)").Text()
		r:=goquerySelection.Find("td:nth-child(3)").Text()
		eproc.CompanyName=p
		eproc.CompanyAddress=q
		eproc.CompanyNpwp=r
	})
	c.OnError(func(r *colly.Response, err error) {
		log.Println("Request URL:", r.Request.URL, "failed with error:", err)
	})
	c.OnRequest(func(r *colly.Request) {
		log.Println("Visiting", r.URL.String())
	})
	c.Visit(url_detail)
}
func price(eproc *eproc, config Config, id string){
	url := config.LPSEUrl
	url_detail := url+"/evaluasi/"+id+"/hasil"
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"),		
	)
	c.Limit(&colly.LimitRule{
		DomainGlob:  "*httpbin.*",
		Parallelism: 2,
		RandomDelay: 20 * time.Second,
	})
	c.OnHTML(".content table", func(e *colly.HTMLElement) {
		goquerySelection := e.DOM
		thead :=map[int]string{}
		goquerySelection.Find("thead").Find("tr:first-child").Find("th").Each(func(i int, s *goquery.Selection){
			thead[i] = s.Text()
		})
		goquerySelection.Find("tbody").Find("tr:first-child").Children().Each(func(i int, s *goquery.Selection){
			if thead[i]=="Penawaran"{
				eproc.BidPrice = tonum(s.Text())
			}else if thead[i]=="Penawaran Terkoreksi"{
				eproc.CorrectedPrice = tonum(s.Text())
			}else if thead[i]=="Hasil Negosiasi"{
				eproc.NegotiationsPrice = tonum(s.Text())
			}
		})
	})

	c.OnError(func(r *colly.Response, err error) {
		log.Println("Request URL:", r.Request.URL, "failed with error:", err)
	})
	
	c.OnRequest(func(r *colly.Request) {
		log.Println("Visiting", r.URL.String())
	})
	c.Visit(url_detail)
}

func update(config Config, data string, id string) (bool, string){
	url := config.EsUrl+"/"+config.EsIndex+"/_doc/"+id
	payload := strings.NewReader(data)
	req, _ := http.NewRequest("PUT", url, payload)

	req.Header.Add("Authorization", "Basic "+config.EsAuth)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "PostmanRuntime/7.15.2")
	req.Header.Add("Accept", "*/*")
	req.Header.Add("Cache-Control", "no-cache")
	req.Header.Add("Postman-Token", "e219990e-f982-42e2-98b2-d90e5a6c812b,731e3fce-d6ce-4381-bd09-7bb1c734bc90")
	req.Header.Add("Host", "71c9d906af1f4f7fafa6945d01722864.ap-southeast-1.aws.found.io:9243")
	req.Header.Add("Accept-Encoding", "gzip, deflate")
	req.Header.Add("Content-Length", "2281")
	req.Header.Add("Connection", "keep-alive")
	req.Header.Add("cache-control", "no-cache")

	res, _ := http.DefaultClient.Do(req)

	if res.Status == "200 OK"{
		log.Println(res.Status)
		return true,res.Status		
	}else{
		log.Println(res.Status)
		return false,res.Status
	}
  }

func shortNum(num float64) string{
	  strnum := ""
	  if num>=1000000 && num<1000000000{
		  num = math.Round(num/1000000)
		  strnum = strconv.FormatFloat(num, 'f', 1, 64)+" Jt"
	  }else if num>=1000000000 && num<1000000000000{
		  num = math.Round(num/1000000000)
		  strnum = strconv.FormatFloat(num, 'f', 1, 64)+" M"
	  }else if num>=1000000000000{
		  num = math.Round(num/1000000000000)
		  strnum = strconv.FormatFloat(num, 'f', 1, 64)+" T"
	  }
	  strnum = strings.Replace(strnum, ".",",", -1)
	return strnum
  }

func tonum(s string) float64{
	s= strings.Replace(s,"Rp ", "",-1)
	s = strings.Replace(s,".", "", -1)
	s = strings.Replace(s,",", ".", -1)
	
	if s, err := strconv.ParseFloat(s, 64); err == nil {
		return s
	}else{
		return 0
	}
}

func loadConfiguration(file string) Config {
    var config Config
    configFile, err := os.Open(file)
    defer configFile.Close()
    if err != nil {
        log.Println(err.Error())
    }
    jsonParser := json.NewDecoder(configFile)
    jsonParser.Decode(&config)
    return config
}

func tojson(ep eproc) string{
	var jsonData, err = json.Marshal(ep)
	if err != nil {
		log.Println(err.Error())
		return ""
	}

	var jsonString = string(jsonData)
	return jsonString
}