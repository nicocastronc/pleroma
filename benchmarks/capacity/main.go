package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// Lag (mensajes) al cierre del escalón que se considera "al día". Cubre la
	// granularidad del worker, que drena de a lotes (~128): dos lotes.
	lagTolerance = 256
	// Si durante el drenaje el conteo de persistidos no crece en este tiempo,
	// se considera estancado (probable pérdida, no lentitud).
	stallAfter = 20 * time.Second
	lineWidth  = 100
)

type config struct {
	dbURL        string
	broker       string
	rates        []int
	stepDur      time.Duration
	drainTimeout time.Duration
	publishers   int
	tenant       string
	installation string
	gateway      string
	device       string
	sensor       string
	csvPath      string
}

type sample struct {
	t         time.Duration
	acked     int64
	persisted int64
}

type stepResult struct {
	Rate         int
	Sent         int
	Acked        int64
	Failed       int64
	PersistedEnd int64
	LagEnd       int64
	LagPeak      int64
	PersistRate  float64 // msg/s persistidos durante la 2a mitad del escalón
	Drain        time.Duration
	Drained      bool
	Stalled      bool
	KeepsUp      bool
}

func parseFlags() config {
	var c config
	var rates string
	flag.StringVar(&c.dbURL, "db", getEnvOrDefault("DATABASE_URL"), "conexión a PostgreSQL/TimescaleDB (o usar env DATABASE_URL)")
	flag.StringVar(&c.broker, "broker", getEnvOrDefault("MQTT_BROKER"), "broker MQTT (o usar env MQTT_BROKER)")
	flag.StringVar(&rates, "rates", "25,50,75,100,125,150,200,250", "tasas a probar (msg/s), separadas por coma")
	flag.DurationVar(&c.stepDur, "step", 20*time.Second, "duración de cada escalón (mínimo 10s)")
	flag.DurationVar(&c.drainTimeout, "drain-timeout", 120*time.Second, "máximo a esperar que se vacíe el backlog tras cada escalón")
	flag.IntVar(&c.publishers, "publishers", 4, "conexiones MQTT que publican en paralelo")
	flag.StringVar(&c.tenant, "tenant", "test", "tenant_id")
	flag.StringVar(&c.installation, "installation", "test", "installation_id")
	flag.StringVar(&c.gateway, "gateway", "test", "gateway_id")
	flag.StringVar(&c.device, "device", "capacity-device", "device_id")
	flag.StringVar(&c.sensor, "sensor", "capacity-sensor", "sensor_id (tiene que estar registrado)")
	flag.StringVar(&c.csvPath, "csv", "", "si se indica, guarda los resultados en este CSV")
	flag.Parse()

	for _, s := range strings.Split(rates, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n <= 0 {
			log.Fatalf("tasa inválida %q", s)
		}
		c.rates = append(c.rates, n)
	}
	sort.Ints(c.rates)
	if c.stepDur < 10*time.Second {
		log.Fatal("-step tiene que ser de al menos 10s")
	}
	if c.publishers < 1 {
		c.publishers = 1
	}
	return c
}

func getEnvOrDefault(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func topicOf(c config) string {
	return fmt.Sprintf("pleroma/v1/%s/%s/%s/telemetry", c.installation, c.gateway, c.device)
}

func payloadOf(c config, seq uint64) []byte {
	b, _ := json.Marshal(map[string]any{
		"schema_version":  "1.0",
		"tenant_id":       c.tenant,
		"installation_id": c.installation,
		"gateway_id":      c.gateway,
		"device_id":       c.device,
		"sensor_id":       c.sensor,
		"sequence":        seq,
		"observed_at":     time.Now().UTC().Format(time.RFC3339Nano),
		"metric":          "temperature",
		"value":           20.0 + rand.Float64(),
		"unit":            "C",
		"quality":         "good",
	})
	return b
}

func countPersisted(ctx context.Context, pool *pgxpool.Pool, sensor string, from, to uint64) int64 {
	var n int64
	err := pool.QueryRow(ctx,
		"SELECT count(*) FROM telemetry WHERE sensor_id = $1 AND sequence >= $2 AND sequence < $3",
		sensor, int64(from), int64(to)).Scan(&n)
	if err != nil {
		log.Printf("\nerror consultando la base: %v", err)
	}
	return n
}

func connectPublishers(c config) []mqtt.Client {
	var out []mqtt.Client
	base := time.Now().UnixNano() % 1000000
	for i := 0; i < c.publishers; i++ {
		opts := mqtt.NewClientOptions().
			AddBroker(c.broker).
			SetClientID(fmt.Sprintf("capacity-%d-%d", base, i)).
			SetCleanSession(true).
			SetConnectTimeout(5 * time.Second)
		cl := mqtt.NewClient(opts)
		tok := cl.Connect()
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			log.Fatalf("no se pudo conectar a MQTT (%s): %v", c.broker, tok.Error())
		}
		out = append(out, cl)
	}
	return out
}

// preflight manda unos mensajes y verifica que llegan a la tabla. Evita interpretar
// como "capacidad" un problema de configuración (sensor no registrado, tabla distinta).
func preflight(ctx context.Context, pool *pgxpool.Pool, cl mqtt.Client, c config, seq uint64) bool {
	const n = 20
	fmt.Printf(" Chequeo previo: %d mensajes de prueba... ", n)
	for i := 0; i < n; i++ {
		tok := cl.Publish(topicOf(c), 1, false, payloadOf(c, seq+uint64(i)))
		tok.WaitTimeout(5 * time.Second)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if countPersisted(ctx, pool, c.sensor, seq, seq+n) >= n {
			fmt.Println("OK (llegan a la base)")
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("FALLÓ")
	fmt.Println(" Los mensajes no aparecen en la tabla telemetry. Revisá:")
	fmt.Println("  - que el sensor esté registrado y activo")
	fmt.Println("  - que el backend esté corriendo y suscripto a ese topic")
	fmt.Println("  - que la tabla y las columnas se llamen así (telemetry: sensor_id, sequence)")
	return false
}

func clearLine() {
	fmt.Print("\r" + strings.Repeat(" ", lineWidth+10) + "\r")
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func runStep(ctx context.Context, pool *pgxpool.Pool, pubs []mqtt.Client, c config, rate int, startSeq uint64, n, total int) stepResult {
	secs := int(c.stepDur.Seconds())
	totalMsgs := rate * secs
	endSeq := startSeq + uint64(totalMsgs)
	topic := topicOf(c)
	P := len(pubs)

	var acked, failed int64
	var ackWG sync.WaitGroup
	var mu sync.Mutex
	var samples []sample

	start := time.Now()
	stop := make(chan struct{})
	done := make(chan struct{})

	// Muestreo cada 1s: cuántos confirmó el broker y cuántos hay en la base.
	go func() {
		defer close(done)
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				p := countPersisted(ctx, pool, c.sensor, startSeq, endSeq)
				a := atomic.LoadInt64(&acked)
				el := time.Since(start)
				mu.Lock()
				samples = append(samples, sample{el, a, p})
				mu.Unlock()
				lag := a - p
				if lag < 0 {
					lag = 0
				}
				fmt.Printf("\r  [%d/%d] %4d msg/s | t=%3.0fs | confirmados %6d | persistidos %6d | lag %6d ",
					n, total, rate, el.Seconds(), a, p, lag)
			}
		}
	}()

	// Publicación: el mensaje idx se programa en idx/rate segundos desde el inicio;
	// los publishers se reparten los índices. Si el reloj se atrasa, recupera enviando
	// sin esperar, así que el promedio se mantiene en la tasa pedida.
	var pubWG sync.WaitGroup
	for p := 0; p < P; p++ {
		pubWG.Add(1)
		go func(p int) {
			defer pubWG.Done()
			for idx := p; idx < totalMsgs; idx += P {
				due := start.Add(time.Duration(float64(idx) / float64(rate) * float64(time.Second)))
				if d := time.Until(due); d > 0 {
					time.Sleep(d)
				}
				tok := pubs[p].Publish(topic, 1, false, payloadOf(c, startSeq+uint64(idx)))
				ackWG.Add(1)
				go func() {
					defer ackWG.Done()
					if tok.WaitTimeout(15*time.Second) && tok.Error() == nil {
						atomic.AddInt64(&acked, 1)
					} else {
						atomic.AddInt64(&failed, 1)
					}
				}()
			}
		}(p)
	}
	pubWG.Wait()
	ackWG.Wait()
	close(stop)
	<-done

	tEnd := time.Since(start)
	ackedN := atomic.LoadInt64(&acked)
	persistedEnd := countPersisted(ctx, pool, c.sensor, startSeq, endSeq)
	lagEnd := ackedN - persistedEnd
	if lagEnd < 0 {
		lagEnd = 0
	}

	// Pico de lag y ritmo de persistencia en la segunda mitad del escalón.
	var half sample
	foundHalf := false
	peak := lagEnd
	mu.Lock()
	for _, s := range samples {
		if !foundHalf && s.t >= c.stepDur/2 {
			half, foundHalf = s, true
		}
		if l := s.acked - s.persisted; l > peak {
			peak = l
		}
	}
	mu.Unlock()
	rate2 := 0.0
	if foundHalf && tEnd > half.t {
		rate2 = float64(persistedEnd-half.persisted) / (tEnd - half.t).Seconds()
	}

	// Drenaje: esperar a que todo lo confirmado esté persistido.
	drainStart := time.Now()
	drained := persistedEnd >= ackedN
	stalled := false
	lastP, lastProgress := persistedEnd, time.Now()
	for !drained {
		time.Sleep(500 * time.Millisecond)
		p := countPersisted(ctx, pool, c.sensor, startSeq, endSeq)
		if p >= ackedN {
			drained = true
			break
		}
		if p > lastP {
			lastP, lastProgress = p, time.Now()
		}
		if time.Since(lastProgress) > stallAfter {
			stalled = true
			break
		}
		if time.Since(drainStart) > c.drainTimeout {
			break
		}
		fmt.Printf("\r  [%d/%d] %4d msg/s | drenando %4.0fs | persistidos %6d de %6d | faltan %6d ",
			n, total, rate, time.Since(drainStart).Seconds(), p, ackedN, ackedN-p)
	}
	drain := time.Duration(0)
	if persistedEnd < ackedN {
		drain = time.Since(drainStart)
	}

	tol := maxInt64(int64(rate), lagTolerance)
	keeps := drained && lagEnd <= tol && rate2 >= 0.85*float64(rate)

	r := stepResult{
		Rate: rate, Sent: totalMsgs, Acked: ackedN, Failed: atomic.LoadInt64(&failed),
		PersistedEnd: persistedEnd, LagEnd: lagEnd, LagPeak: peak, PersistRate: rate2,
		Drain: drain, Drained: drained, Stalled: stalled, KeepsUp: keeps,
	}
	clearLine()
	fmt.Printf("  [%d/%d] %4d msg/s -> confirmados %d | persistidos al cierre %d | lag %d (pico %d) | persist/s %.0f | %s | %s\n",
		n, total, rate, r.Acked, r.PersistedEnd, r.LagEnd, r.LagPeak, r.PersistRate, drainText(r), status(r))
	return r
}

func status(r stepResult) string {
	switch {
	case r.Stalled:
		return "ESTANCADO"
	case !r.Drained:
		return "NO DRENÓ"
	case r.KeepsUp:
		return "al día"
	default:
		return "acumula"
	}
}

func drainText(r stepResult) string {
	switch {
	case r.Stalled:
		return fmt.Sprintf("estancado tras %.0fs", r.Drain.Seconds())
	case !r.Drained:
		return fmt.Sprintf("no drenó en %.0fs", r.Drain.Seconds())
	case r.Drain == 0:
		return "sin backlog"
	default:
		return fmt.Sprintf("drenó en %.1fs", r.Drain.Seconds())
	}
}

func printHeader(c config) {
	line := strings.Repeat("=", lineWidth)
	fmt.Println(line)
	fmt.Println(" PLEROMA - PRUEBA DE CAPACIDAD ESCALONADA")
	fmt.Println(line)
	fmt.Printf(" Fecha:       %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Printf(" Broker:      %s (%d conexiones publicando, QoS 1)\n", c.broker, c.publishers)
	fmt.Printf(" Destino:     sensor %q, topic %s\n", c.sensor, topicOf(c))
	fmt.Printf(" Escalones:   %v msg/s, %s cada uno\n", c.rates, c.stepDur)
	fmt.Printf(" Entre escalones se espera a que el backlog llegue a 0 (máximo %s).\n", c.drainTimeout)
	fmt.Println()
	fmt.Println(" Cómo leerlo:")
	fmt.Println("  - Confirmados: mensajes que el broker acusó (ACK). No es lo mismo que persistidos.")
	fmt.Println("  - Lag pico: El máximo backlog observado (muestreado cada 1s).")
	fmt.Println("  - Lag al cierre: confirmados que todavía no están en la base al finalizar el escalón.")
	fmt.Println("  - Persist/s: ritmo de guardado en la 2a mitad del escalón.")
	fmt.Printf("  - 'al día' = lag al cierre <= max(1 s de tráfico, %d mensajes) y persist/s >= 85%% de la tasa.\n", lagTolerance)
	fmt.Println(line)
}

func printTable(rs []stepResult) {
	line := strings.Repeat("=", lineWidth)
	fmt.Println()
	fmt.Println(line)
	fmt.Println(" RESULTADOS POR ESCALÓN")
	fmt.Println(line)
	fmt.Printf("%9s | %10s | %11s | %8s | %8s | %10s | %9s | %s\n",
		"Tasa", "Confirmados", "Persistidos", "Lag al", "Lag", "Persist/s", "Drenaje", "Estado")
	fmt.Printf("%9s | %10s | %11s | %8s | %8s | %10s | %9s |\n",
		"(msg/s)", "(broker)", "al cierre", "cierre", "pico", "(2a mitad)", "(s)")
	fmt.Println(strings.Repeat("-", lineWidth))
	failed := int64(0)
	for _, r := range rs {
		d := fmt.Sprintf("%.1f", r.Drain.Seconds())
		if r.Stalled {
			d = "estancado"
		} else if !r.Drained {
			d = fmt.Sprintf(">%.0f", r.Drain.Seconds())
		}
		fmt.Printf("%9d | %10d | %11d | %8d | %8d | %10.0f | %9s | %s\n",
			r.Rate, r.Acked, r.PersistedEnd, r.LagEnd, r.LagPeak, r.PersistRate, d, status(r))
		failed += r.Failed
	}
	fmt.Println(strings.Repeat("-", lineWidth))
	if failed > 0 {
		fmt.Printf(" Ojo: %d publicaciones no fueron confirmadas por el broker (conexión caída o timeout).\n", failed)
	}
}

func printConclusion(rs []stepResult, c config, aborted bool) {
	if len(rs) == 0 {
		return
	}
	fmt.Println()
	fmt.Println(" CONCLUSIÓN (calculada con los datos de arriba)")
	fmt.Println(strings.Repeat("-", lineWidth))

	lastOK, firstAccum := 0, 0
	for _, r := range rs {
		if firstAccum != 0 {
			break
		}
		if r.KeepsUp {
			lastOK = r.Rate
		} else {
			firstAccum = r.Rate
		}
	}
	switch {
	case firstAccum == 0:
		fmt.Printf(" - No se encontró el límite: todos los escalones (hasta %d msg/s) terminaron al día. Probá tasas más altas.\n", rs[len(rs)-1].Rate)
	case lastOK == 0:
		fmt.Printf(" - Ya el primer escalón (%d msg/s) acumula backlog. Probá tasas más bajas.\n", firstAccum)
	default:
		fmt.Printf(" - Sin backlog sostenido hasta %d msg/s. Desde %d msg/s el backlog crece durante el escalón.\n", lastOK, firstAccum)
		fmt.Printf("   El límite está entre %d y %d msg/s; para afinarlo, probá tasas intermedias.\n", lastOK, firstAccum)
	}

	var est []float64
	for _, r := range rs {
		if r.Drained && r.LagEnd > 0 && r.Drain >= 2*time.Second {
			est = append(est, float64(r.LagEnd)/r.Drain.Seconds())
		}
	}
	if len(est) > 0 {
		sort.Float64s(est)
		fmt.Printf(" - Capacidad de persistencia estimada (ritmo de drenaje): ~%.0f msg/s (rango %.0f-%.0f, n=%d escalones).\n",
			est[len(est)/2], est[0], est[len(est)-1], len(est))
	} else {
		fmt.Println(" - Capacidad de persistencia: sin datos suficientes (ningún escalón dejó un backlog que se pudiera medir al drenar).")
	}

	drainedSteps := 0
	for _, r := range rs {
		if r.Drained {
			drainedSteps++
		}
	}
	fmt.Printf(" - Reconciliación: en %d de %d escalones ejecutados, todo lo confirmado por el broker quedó persistido.\n", drainedSteps, len(rs))

	if aborted {
		last := rs[len(rs)-1]
		if last.Stalled {
			fmt.Printf(" - [!] En %d msg/s el conteo de persistidos dejó de crecer con mensajes sin persistir: probable pérdida antes de la base (broker o transporte), no lentitud.\n", last.Rate)
		} else {
			fmt.Printf(" - [!] En %d msg/s el backlog no se vació en %s: la persistencia de ese escalón NO está verificada.\n", last.Rate, c.drainTimeout)
		}
		fmt.Println("   La prueba se detuvo ahí: seguir habría mezclado ese backlog con los escalones siguientes.")
	}

	fmt.Println()
	fmt.Println(" LIMITACIONES Y METODOLOGÍA (para citar junto con estos resultados)")
	fmt.Println(strings.Repeat("-", lineWidth))
	fmt.Println(" - Entorno: Broker, backend, base y generador en la misma máquina (localhost).")
	fmt.Println(" - Casuística: Publicación concentrada en un solo sensor y dispositivo (peor caso de paralelismo).")
	fmt.Printf(" - Muestreo: 'Lag pico' es el máximo observado en lecturas cada 1s, no un registro continuo.\n")
	fmt.Printf(" - Capacidad Estimada: La tasa de drenaje es una extrapolación matemática basada en el tiempo de vaciado.\n")
	fmt.Printf(" - Criterio 'Al día': Admite hasta %d mensajes de lag final (o 1s de tráfico) y exige ritmo de persistencia >= 85%%.\n", lagTolerance)
	fmt.Println(" - Fiabilidad: Una sola corrida de 20s. Para uso en producción se recomiendan múltiples corridas.")
	fmt.Println(strings.Repeat("=", lineWidth))
}

func writeCSV(path string, rs []stepResult) {
	f, err := os.Create(path)
	if err != nil {
		log.Printf("no se pudo crear %s: %v", path, err)
		return
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"rate_msg_s", "confirmed", "failed_publish", "persisted_at_end", "lag_end", "lag_peak",
		"persist_rate_2nd_half", "drain_s", "drained", "stalled", "keeps_up"})
	for _, r := range rs {
		w.Write([]string{
			strconv.Itoa(r.Rate), strconv.FormatInt(r.Acked, 10), strconv.FormatInt(r.Failed, 10),
			strconv.FormatInt(r.PersistedEnd, 10), strconv.FormatInt(r.LagEnd, 10), strconv.FormatInt(r.LagPeak, 10),
			fmt.Sprintf("%.1f", r.PersistRate), fmt.Sprintf("%.1f", r.Drain.Seconds()),
			strconv.FormatBool(r.Drained), strconv.FormatBool(r.Stalled), strconv.FormatBool(r.KeepsUp),
		})
	}
	fmt.Printf(" Resultados guardados en %s\n", path)
}

func main() {
	c := parseFlags()
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, c.dbURL)
	if err != nil {
		log.Fatalf("base de datos: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("no se pudo conectar a la base: %v", err)
	}

	pubs := connectPublishers(c)
	defer func() {
		for _, p := range pubs {
			p.Disconnect(250)
		}
	}()

	printHeader(c)

	// Base de secuencias única por corrida: evita chocar con filas de corridas anteriores
	// (la deduplicación por sequence las contaría como "persistidas" sin serlo).
	seq := uint64(time.Now().UnixNano())
	if !preflight(ctx, pool, pubs[0], c, seq) {
		os.Exit(1)
	}
	seq += 1000

	var results []stepResult
	aborted := false
	for i, rate := range c.rates {
		r := runStep(ctx, pool, pubs, c, rate, seq, i+1, len(c.rates))
		seq += uint64(r.Sent) + 1000
		results = append(results, r)
		if !r.Drained {
			aborted = true
			break
		}
		if i < len(c.rates)-1 {
			time.Sleep(3 * time.Second)
		}
	}

	printTable(results)
	printConclusion(results, c, aborted)
	if c.csvPath != "" {
		writeCSV(c.csvPath, results)
	}
}
