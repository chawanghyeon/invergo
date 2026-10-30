# DuckDB FTS 벤치마크

DuckDB `fts` 확장(BM25)만으로 경량 검색 서버를 만들 수 있는지 판단하기 위한 벤치마크입니다.
합성 코퍼스(Zipf 분포)를 만들어 아래 항목을 측정하고 결과를 JSON으로 저장합니다.

> 커밋된 결과는 **DuckDB 1.5.5 + 정적 인덱스**(10만 건, 100만 건)와
> **DuckDB 2.0.0의 세 조건**(incremental, incremental + `cluster_terms`, 정적. 모두 100만 건)입니다.
> 2.0.0 결과와 1.5.5 비교는 [아래](#결과-duckdb-200과-155-비교-100만-건)에 있습니다.

| 측정 항목 | 내용 |
|:--|:--|
| 적재 / 인덱싱 | 문서 적재 시간, `create_fts_index` 생성 시간 |
| 검색 지연 | 단일 클라이언트 BM25 top-k의 p50 / p95 / p99 (쿼리 종류별) |
| 동시 처리 | 워커 수별 QPS와 지연 분포 |
| 실시간 업데이트 | 배치/단건 INSERT, DELETE 비용, 인덱스 반영 여부, 전체 재빌드 시간 |
| 메모리 | 프로세스 RSS, 피크 RSS, DuckDB 버퍼 사용량 |
| 저장 효율 | 적재 후 / 인덱스 후 / 업데이트 후 DB 파일 크기 |

## 실행

```bash
pip install -r requirements.txt        # duckdb==2.0.0
python duckdb_fts_benchmark.py \
  --rows 1000000 --append 100000 \
  --workers 1,4,8 --threads 4 \
  --out results/duckdb-2.0.0_incremental_1m.json
```

`fts` 확장은 pip 패키지에 포함되어 있지 않습니다. 처음 `LOAD`/`INSTALL`할 때
`extensions.duckdb.org`에서 내려받으므로 그 호스트에 접속할 수 있어야 합니다.
이미 받은 확장 파일이 있으면 `--fts-extension path/to/fts.duckdb_extension`으로 지정합니다.

HTTPS 프록시를 거쳐야만 외부에 나갈 수 있는 환경(이번 측정 환경)에서는 그 호스트를 허용해도 기본 `INSTALL fts`가 실패합니다.
DuckDB는 기본 저장소에 `http://`로 접속하는데, 내장 HTTP 클라이언트가 `HTTPS_PROXY` 환경 변수를 읽지 않기 때문입니다.
HTTPS 저장소를 쓰려면 `httpfs` 확장이 필요한데, `httpfs`도 내려받아야 하는 확장입니다.
그래서 `httpfs`만 먼저 curl로 받아 파일로 설치한 뒤, 프록시를 지정하고 `fts`를 설치합니다.
서명 검사(`allow_unsigned_extensions = false`)는 켜 둔 채로 설치됩니다.
한 번 설치하면 `~/.duckdb/extensions/v2.0.0/<platform>/`에 남으므로, 이후에는 스크립트의 `LOAD fts`가 그대로 동작합니다.

```bash
curl -sSO https://extensions.duckdb.org/v2.0.0/linux_amd64/httpfs.duckdb_extension.gz
gunzip httpfs.duckdb_extension.gz
python - <<'EOF'
import duckdb
con = duckdb.connect()
con.execute("INSTALL 'httpfs.duckdb_extension'")
con.execute("LOAD httpfs")
con.execute("SET http_proxy = '<프록시 host:port>'")
con.execute("SET ca_cert_file = '<프록시가 TLS를 다시 서명한다면 그 CA 번들>'")
con.execute("SET custom_extension_repository = 'https://extensions.duckdb.org'")
con.execute("INSTALL fts")
EOF
```

오프라인 환경에서는 DuckDB 1.5.5와 PyPI의 `duckdb-extension-fts` 휠로 실행할 수 있습니다.
이 휠은 서드파티가 재패키징한 것이지만, 서명 검사를 켠 기본 설정
(`allow_unsigned_extensions = false`)에서 로드되므로 DuckDB가 서명한 공식 바이너리입니다.

```bash
pip install duckdb==1.5.5 duckdb-extension-fts==1.5.5
python duckdb_fts_benchmark.py --rows 1000000 --append 100000 \
  --fts-extension "$(python -c 'import duckdb_extension_fts, pathlib; print(next(pathlib.Path(duckdb_extension_fts.__file__).parent.rglob("fts.duckdb_extension")))')"
```

주요 옵션:

| 옵션 | 기본값 | 설명 |
|:--|:--|:--|
| `--rows` | 100000 | 초기 문서 수 |
| `--append` / `--append-batch` | 10000 / 1000 | 업데이트 단계에서 추가할 문서 수 / INSERT 한 번에 넣는 행 수 |
| `--single-inserts` | 100 | 시간을 잴 단건 INSERT 횟수 |
| `--delete` | append/10 | 삭제할 문서 수 |
| `--workers` | `1,4,8` | 동시성 단계의 워커 수 목록 |
| `--threads` | CPU 수 | DuckDB `threads` 설정 |
| `--duration` | 15 | 동시성 측정 1회당 초 |
| `--queries` | 50 | 지연 측정 시 쿼리 종류별 쿼리 수 |
| `--incremental` | `auto` | `auto`: 지원되면 `incremental=1`, 아니면 정적 인덱스로 대체 |
| `--cluster-terms` | off | `cluster_terms=1` 전달 (2.0 FTS) |
| `--conjunctive` | off | 다중 단어 쿼리를 AND 조건으로 실행 |
| `--fts-extension` / `--allow-unsigned` | - | 로드할 확장 파일 경로 / 서명 없는(직접 빌드한) 확장 허용 |

## 측정 방법

- **코퍼스**: 알파벳 단어 10만 개 사전에서 문서당 20~120단어(평균 70)를 뽑습니다.
  단어 순위는 Zipf(s=1) 분포(`rank = floor(V^u)`)를 따르고, `hash()` 기반이라 같은 시드면 같은 데이터가 나옵니다.
  기본 토크나이저가 숫자를 지우므로 단어에는 알파벳만 씁니다.
- **쿼리 종류**:
  - `head`: 상위 0.1% 단어 (문서 대부분에 등장, posting list가 김)
  - `mid`: 상위 0.1~10% 단어
  - `tail`: 나머지 희귀 단어
  - `multi3`: 문서와 같은 분포에서 뽑은 3단어 OR 쿼리
- **쿼리 형태**: `match_bm25(id, ?)`로 점수를 매기고 `ORDER BY score DESC LIMIT 10`으로 상위 10건을 가져옵니다.
  쿼리마다 prepared parameter를 씁니다.
- **동시성**: 워커마다 `con.cursor()`로 별도 커서를 엽니다. 네 종류 쿼리를 섞어 `--duration`초 동안 반복합니다.
- **업데이트**: 추가 문서는 미리 스테이징 테이블에 만들어 두고, 같은 INSERT 문을 FTS 인덱스가 없는 빈 테이블에도 실행해 비교 기준으로 씁니다.
  추가 문서에는 표시용 단어(`zqxvjupdatemarker`)가 들어 있어, 그 단어로 검색되는 건수로 인덱스 반영 여부를 정확히 셉니다.
  각 측정 구간 전에는 `CHECKPOINT`를 실행해 자동 체크포인트가 한쪽 측정에만 걸리지 않게 합니다.

## 결과: DuckDB 1.5.5, 정적 인덱스

환경은 4 vCPU, 15.7 GB RAM, Linux x86_64, Python 3.13이고 실행 옵션은 `--threads 4`입니다.
fts 확장은 `6814ec9`(공식 서명 바이너리)입니다.
원본 JSON은 [`results/`](results/)에 있습니다.

### 적재, 인덱싱, 저장 용량

| 문서 수 | 토큰 수 | 원문 | 적재 | 인덱스 생성 | 테이블 파일 | 인덱스 후 파일 | 인덱스 생성 피크 RSS |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 100,000 | 7.0M | 53 MB | 0.6 s | 8.1 s | 34 MB | 80 MB (+46 MB) | 0.48 GB |
| 1,000,000 | 70.0M | 530 MB | 14.0 s | 35.0 s | 325 MB | 769 MB (+445 MB) | 3.5 GB |

### 단일 클라이언트 검색 지연 (top-10)

`mean df`는 쿼리 단어가 등장하는 평균 문서 수입니다.

| 쿼리 | 100k mean df | 100k p50 / p95 / p99 | 1M mean df | 1M p50 / p95 / p99 |
|:--|--:|--:|--:|--:|
| head (1단어) | 19,897 | 37 / 50 / 52 ms | 183,544 | 153 / 313 / 415 ms |
| mid (1단어) | 304 | 28 / 33 / 82 ms | 2,301 | 78 / 85 / 95 ms |
| tail (1단어) | 17 | 22 / 26 / 29 ms | 151 | 68 / 73 / 75 ms |
| multi3 (3단어 OR) | - | 63 / 100 / 113 ms | - | 504 / 922 / 10,959 ms* |

\* 1M multi3에서는 30개 중 1개 쿼리가 15.0초 걸렸습니다. 같은 30개 쿼리를 두 번씩(60회) 다시 실행했을 때는 재현되지 않았습니다(최대 874 ms, 중앙값 558 ms).
업데이트 후 다시 측정한 multi3는 p50 437 ms, p99 685 ms였습니다.
쿼리 종류당 표본이 30~50개뿐이라 p99는 사실상 최댓값입니다.

### 동시 검색 (네 종류 쿼리 혼합)

| 워커 | 100k QPS | 100k p50 / p99 | 1M QPS | 1M p50 / p99 |
|--:|--:|--:|--:|--:|
| 1 | 25.7 | 31 / 93 ms | 5.5 | 87 / 650 ms |
| 4 | 49.4 | 65 / 227 ms | 7.5 | 314 / 1,770 ms |
| 8 | 51.7 | 117 / 500 ms | 6.0 | 736 / 4,848 ms |

### 업데이트 (정적 인덱스)

| 항목 | 100k | 1M |
|:--|--:|--:|
| 추가 문서 수 (1,000행 배치 + 단건 100) | 10,100 | 100,100 |
| 추가 후 검색되는 문서 수 | **0** | **0** |
| BM25 통계 `num_docs` vs 실제 행 수 | 100,000 vs 109,100 | 1,000,000 vs 1,090,100 |
| 단건 INSERT p50 | 2.8 ms | 2.7 ms |
| 전체 재빌드 (`overwrite=1`) | 11.6 s | 56.9 s |

정적 인덱스에는 트리거가 없으므로 INSERT 속도는 인덱스가 없는 테이블과 같습니다(0.9배~1.3배는 측정 노이즈 범위).
삭제한 문서는 `match_bm25`가 `docs` 테이블과 조인되기 때문에 결과에서 빠집니다.
하지만 df, 문서 수, 평균 길이 같은 BM25 통계는 재빌드 전까지 이전 값 그대로입니다.

### 결과에서 보이는 점

1. **희귀어 검색도 비용이 문서 수에 비례합니다.**
   - df가 151인 tail 단어도 1M에서 68 ms 걸립니다(100k에서는 22 ms).
   - 1.5.5의 `match_bm25`는 `docs` 행마다 평가되는 매크로입니다. 그래서 쿼리마다 `docs`와 `terms` 테이블을 스캔합니다.
   - posting list를 읽고 top-k에서 일찍 멈추는 Lucene/Elasticsearch와 구조가 다릅니다.
2. **동시성으로 처리량이 늘지 않습니다.**
   - 쿼리 하나가 이미 4개 스레드를 다 씁니다.
   - 1M 기준 워커 1→4개에서 QPS는 5.5→7.5로 1.4배 늘었습니다. 8개에서는 6.0으로 오히려 줄었고 p99는 4.8초까지 올라갔습니다.
3. **인덱스가 원문 크기와 비슷한 공간을 씁니다.** 1M 기준 원문 530 MB에 대해 인덱스가 445 MB입니다.
   인덱스 생성 중 피크 RSS는 3.5 GB, 재빌드까지 포함한 프로세스 피크는 4.8 GB입니다.
4. **1.5.5 수치를 2.0.0에 그대로 적용하면 안 됩니다.**
   2.0.0의 fts는 `match_bm25`를 다시 구현했습니다(BM25F, `cluster_terms`, layered search, incremental 등).
   실제로 같은 조건에서 2.0.0의 검색이 2~4배 느렸습니다([아래](#결과-duckdb-200과-155-비교-100만-건)).

## 결과: DuckDB 2.0.0과 1.5.5 비교 (100만 건)

환경과 옵션은 위 1.5.5 100만 건 측정과 같습니다.
4 vCPU, 15.7 GB RAM, Python 3.13, `--threads 4 --duration 20 --queries 30`이고, 업데이트 단계는 1,000행 배치로 100,000건 추가, 단건 INSERT 100회, 삭제 10,000건입니다.
DuckDB는 `v2.0.0`(`d54cbbe3df`)이고 fts는 `INSTALL fts`로 `extensions.duckdb.org`에서 받은 공식 서명 바이너리(`223b8cb9e6`)입니다.
세 조건은 같은 머신에서 하나씩 차례로, 매번 새 프로세스와 새 DB 파일로 실행했습니다.
데이터와 쿼리는 시드가 같아 1.5.5와 동일합니다. 그래서 쿼리 종류별 mean df도 같습니다(head 183,544, mid 2,301, tail 151).

```bash
COMMON="--rows 1000000 --append 100000 --workers 1,4,8 --threads 4 --duration 20 --queries 30"
python duckdb_fts_benchmark.py $COMMON --incremental on                 --out results/duckdb-2.0.0_incremental_1m.json
python duckdb_fts_benchmark.py $COMMON --incremental on --cluster-terms --out results/duckdb-2.0.0_incremental_clustered_1m.json
python duckdb_fts_benchmark.py $COMMON --incremental off                --out results/duckdb-2.0.0_static_1m.json
```

### 적재, 인덱싱, 저장 용량

적재 직후 테이블 파일은 네 조건 모두 324 MB입니다.

| 조건 | 적재 | 인덱스 생성 | 인덱스 후 파일 | 인덱스 생성 피크 RSS |
|:--|--:|--:|--:|--:|
| 1.5.5 정적 | 14.0 s | 35.0 s | 769 MB (+445 MB) | 3.5 GB |
| 2.0.0 incremental | 22.2 s | 40.8 s | 493 MB (+169 MB) | 3.5 GB |
| 2.0.0 incremental + `cluster_terms` | 18.0 s | 43.0 s | 538 MB (+214 MB) | 3.8 GB |
| 2.0.0 정적 | 17.5 s | 32.4 s | 485 MB (+161 MB) | 3.5 GB |

적재는 fts와 관계없는 같은 SQL입니다. 2.0.0의 세 값(17.5~22.2 s)이 서로 크게 다르므로, 1.5.5(14.0 s)와의 차이를 버전 차이로 단정하기는 어렵습니다.

### 단일 클라이언트 검색 지연 (top-10, p50 / p95 / p99, ms)

| 조건 | head (1단어) | mid (1단어) | tail (1단어) | multi3 (3단어 OR) |
|:--|--:|--:|--:|--:|
| 1.5.5 정적 | 153 / 313 / 415 | 78 / 85 / 95 | 68 / 73 / 75 | 504 / 922 / 10,959* |
| 2.0.0 incremental | 342 / 572 / 703 | 278 / 380 / 397 | 255 / 317 / 337 | 700 / 1,326 / 1,480 |
| 2.0.0 incremental + `cluster_terms` | 312 / 541 / 625 | 232 / 296 / 356 | 234 / 296 / 339 | 633 / 1,149 / 1,337 |
| 2.0.0 정적 | 333 / 572 / 659 | 256 / 310 / 335 | 244 / 302 / 332 | 760 / 1,280 / 1,474 |

\* 위 1.5.5 결과에서 설명한 15초짜리 이상치 1건 때문입니다. 2.0.0 세 조건의 multi3 최댓값은 1.4~1.5초였습니다.
쿼리 종류당 표본이 30개라 p99는 사실상 최댓값입니다.

업데이트 단계가 끝난 뒤 다시 잰 지연입니다(p50 / p99, ms).
정적 인덱스는 추가 문서가 반영되지 않은 100만 건 인덱스를 검색하고, incremental 인덱스는 추가·삭제가 반영된 1,090,100건 인덱스를 검색합니다.

| 조건 | head | mid | tail | multi3 |
|:--|--:|--:|--:|--:|
| 1.5.5 정적 | 140 / 369 | 64 / 75 | 58 / 70 | 437 / 685 |
| 2.0.0 incremental | 384 / 981 | 289 / 347 | 280 / 468 | 1,055 / 1,881 |
| 2.0.0 incremental + `cluster_terms` | 289 / 679 | 218 / 338 | 222 / 360 | 764 / 1,423 |
| 2.0.0 정적 | 356 / 732 | 251 / 341 | 253 / 355 | 705 / 1,678 |

### 동시 검색 (네 종류 쿼리 혼합, QPS와 p50 / p99)

| 워커 | 1.5.5 정적 | 2.0.0 incremental | 2.0.0 incremental + `cluster_terms` | 2.0.0 정적 |
|--:|--:|--:|--:|--:|
| 1 | 5.5 (87 / 650 ms) | 2.6 (311 / 1,298 ms) | 3.0 (264 / 1,171 ms) | 2.6 (278 / 1,430 ms) |
| 4 | 7.5 (314 / 1,770 ms) | 2.7 (1,229 / 3,473 ms) | 3.7 (916 / 2,959 ms) | 3.1 (1,136 / 3,094 ms) |
| 8 | 6.0 (736 / 4,848 ms) | 2.4 (2,459 / 8,368 ms) | 2.9 (1,871 / 6,271 ms) | 2.3 (2,684 / 9,169 ms) |

2.0.0은 측정 1회(20초)에 쿼리가 50~75개뿐이라 p99는 사실상 최댓값입니다. 모든 단계에서 오류는 없었습니다.

### 업데이트

| 항목 | 1.5.5 정적 | 2.0.0 incremental | 2.0.0 incremental + `cluster_terms` | 2.0.0 정적 |
|:--|--:|--:|--:|--:|
| 1,000행 배치 INSERT 100회 (100,000건) | 2.2 s | 104.6 s | 256.3 s | 3.3 s |
| 배치 INSERT 처리량 | 46,148 rows/s | 956 rows/s | 390 rows/s | 30,748 rows/s |
| 인덱스 없는 테이블 대비 | 1.3배 | 45.8배 | 110.8배 | 1.5배 |
| 단건 INSERT p50 / p99 | 2.7 / 6.1 ms | 626 / 812 ms | 2,093 / 2,415 ms | 4.8 / 6.6 ms |
| 인덱스 없는 테이블의 단건 INSERT p50 | 2.6 ms | 3.9 ms | 3.5 ms | 4.6 ms |
| 추가 후 검색되는 문서 수 (기대값 100,100) | **0** | **100,100** | **100,100** | **0** |
| 10,000건 DELETE | 0.003 s | 2.04 s | 3.59 s | 0.006 s |
| 삭제 후 검색되는 추가 문서 수 (기대값 90,100) | 0 | 90,100 | 90,100 | 0 |
| BM25 `num_docs` (실제 행 수 1,090,100) | 1,000,000 | 1,090,100 | 1,090,100 | 1,000,000 |
| 업데이트 후 DB 파일 | 769 MB | 627 MB | 577 MB | 607 MB |
| 전체 재빌드 (`overwrite=1`) | 56.9 s | 46.0 s | 49.7 s | 45.0 s |
| 재빌드 후 DB 파일 | 793 MB | 740 MB | 749 MB | 694 MB |
| 프로세스 피크 RSS | 4.8 GB | 4.0 GB | 4.4 GB | 4.0 GB |

재빌드는 처음과 같은 옵션으로 실행했습니다. incremental 조건은 `incremental=1`(과 `cluster_terms=1`)을 다시 지정했습니다.
정적 인덱스에서 삭제 후 검색되는 수가 0인 것은 추가 문서가 처음부터 인덱스에 없기 때문입니다.

### 결과에서 보이는 점 (2.0.0)

1. **2.0.0은 1.5.5보다 검색이 2~4배 느립니다. incremental 여부와는 거의 관계가 없습니다.**
   - 정적 인덱스끼리 비교해도 p50이 tail 68→244 ms(3.6배), mid 78→256 ms(3.3배), head 153→333 ms(2.2배), multi3 504→760 ms(1.5배)로 늘었습니다.
   - 2.0.0 안에서 incremental과 정적의 p50 차이는 ±10% 이내입니다. 쿼리 종류당 표본 30개의 노이즈와 구분되지 않는 수준입니다.
   - 동시 QPS도 1.5.5의 5.5~7.5에서 2.3~3.7로 떨어졌습니다. 1.5.5처럼 워커를 늘려도 처리량은 거의 늘지 않고 지연만 커집니다.
   - 2.0.0의 `match_bm25`도 1.5.5처럼 `docs` 행마다 평가되는 매크로입니다. 다만 정의가 훨씬 커졌습니다. 인자 검증, BM25F와 best_fields 점수 계산, 필드별 길이 정규화가 추가됐습니다.
     원인은 쿼리당 고정 비용이 커진 것으로 보입니다(추정, 프로파일링은 하지 않았습니다). df가 가장 작은 tail 쿼리에서 배율이 가장 큰 것도 이 설명과 맞습니다.
   - 1.5.5에서 한 번 나온 15초짜리 multi3 이상치는 2.0.0 세 조건에서는 나오지 않았습니다.
2. **`cluster_terms=1`은 검색을 조금 빠르게 하지만 쓰기를 크게 느리게 합니다.**
   - 검색: incremental만 켠 경우보다 p50이 8~17% 낮고, 동시 QPS는 17~35% 높았습니다. 세 조건 중 가장 빨랐지만 1.5.5보다는 여전히 느립니다.
   - 쓰기: 배치 INSERT는 2.5배(105→256 s), 단건 INSERT는 3.3배(626→2,093 ms), DELETE는 1.8배(2.0→3.6 s) 느렸습니다.
   - 이 옵션을 켜면 `terms`는 termid 순으로 정렬된 `__terms_storage` 테이블을 보여 주는 뷰가 됩니다.
     INSERT/DELETE 트리거는 docid로 이 테이블과 `docs`를 조인해서 바뀐 문서의 term을 찾습니다. 저장 순서가 docid 순이 아니어서 더 많이 읽게 되는 것으로 보입니다(추정).
3. **incremental 인덱스는 반영이 정확하지만 쓰기 비용이 매우 큽니다.**
   - 추가한 100,100건과 삭제한 10,000건이 곧바로 검색 결과와 BM25 통계(`num_docs`)에 반영됐습니다.
   - 대신 1,000행 배치 INSERT가 인덱스 없는 테이블보다 46배 느리고(956 rows/s), 단건 INSERT는 p50 626 ms입니다.
   - 트리거는 `FOR EACH STATEMENT`이고, INSERT 문 하나마다 트리거 5개(docs, dict 추가, terms, dict df, stats)가 실행됩니다.
     df를 고치는 트리거는 `terms`(토큰 수만큼, 약 7,000만 행)와 `docs`를 조인합니다. 그래서 문 하나의 고정 비용이 인덱스 크기에 따라 커지는 것으로 보입니다(추정).
     2만 건으로 돌린 스모크 테스트에서는 단건 INSERT p50이 65 ms였습니다(참고용, 커밋하지 않음).
   - 100,000건을 1,000행씩 incremental로 넣는 시간(105 s, `cluster_terms` 256 s)이 1,090,100건 전체를 재빌드하는 시간(45~50 s)보다 깁니다.
     배치를 더 크게 잡으면 문당 고정 비용이 줄 것으로 보이지만 이번에는 측정하지 않았습니다.
4. **인덱스는 작아졌습니다.**
   - 인덱스가 차지하는 공간이 445 MB에서 161~214 MB로, 1.5.5의 36~48% 수준입니다.
   - 재빌드도 56.9 s에서 45~50 s로 빨라졌습니다.
   - 프로세스 피크 RSS도 4.8 GB에서 4.0~4.4 GB로 줄었지만, 인덱스 생성 중 피크(3.5~3.8 GB)는 1.5.5와 비슷합니다.
5. **2.0.0으로 올려서 얻는 것은 실시간 반영, 작은 인덱스, BM25F입니다. 검색 속도는 아닙니다.**
   - 100만 건에서 단일 쿼리 p50이 0.23~0.76초이고 동시 QPS는 2~4입니다. 대화형 검색 서버로 쓰기에는 1.5.5보다도 불리합니다.
   - 쓰기가 잦다면 incremental보다 정적 인덱스와 주기적 재빌드를 쓰는 편이 이 측정에서는 더 빨랐습니다.

## 이전 답변 검증

| 이전 답변의 내용 | 확인 결과 |
|:--|:--|
| "외부 패키지 다운로드가 차단되어 DuckDB 2.0.0 설치 실패" | 이 환경에서는 `pip install duckdb==2.0.0`이 성공합니다(PyPI에 2.0.0 존재, `source_id d54cbbe3df` = 태그 `v2.0.0`). 막힌 것은 fts 확장을 받는 `extensions.duckdb.org`입니다. pip 설치에 성공해도 `LOAD fts`에서 실패합니다. 이후 이 호스트를 허용해 2.0.0도 측정했습니다([아래](#duckdb-200-fts-설치-확인)). |
| "공식 2.0 문서에서 incremental FTS와 `match_bm25` 지원 확인" | 맞습니다. DuckDB v2.0.0이 고정한 duckdb-fts 커밋 `2266e67d`의 README와 소스에 `create_fts_index(..., incremental = 0)` 옵션과 `match_bm25`가 있습니다. `INSTALL fts`로 받은 실제 바이너리(`223b8cb9e6`)에서도 `incremental=1`, `cluster_terms=1`, `match_bm25`가 동작했습니다. 제약은 아래에 정리했습니다. |
| "벤치마크 코드 작성, 문법 검사 완료" | 전달된 내용에는 설명만 있고 코드는 이 저장소에 없습니다. 이 디렉터리의 스크립트를 새로 작성했고 실제로 실행했습니다. |
| 실행 방법 `pip install duckdb==2.0.0` 후 실행 | 이것만으로는 부족합니다. fts 확장은 pip 패키지에 들어 있지 않고 런타임에 `extensions.duckdb.org`에서 받아야 합니다. HTTPS 프록시 환경에서는 [실행](#실행)에 적은 설치 절차가 더 필요합니다. |
| "실제 쿼리 실행 및 성능 측정 미완료" | 1.5.5와 공식 서명 fts로 10만 건과 100만 건을 실측했습니다. 2.0.0도 공식 서명 fts로 세 조건을 100만 건씩 실측했습니다(위). |
| "10만 → 100만 → 1,000만 건 확장 가능" | 스크립트는 `--rows`로 늘릴 수 있습니다. 다만 1M에서 이미 프로세스 피크 RSS가 4.8 GB였으므로, 1,000만 건은 16 GB 머신에서 메모리가 부족할 가능성이 높습니다(미측정). |

### 2.0 incremental FTS의 제약

fts `2266e67d`의 README와 소스를 기준으로 정리했습니다.
"확인"이라고 적은 항목은 `INSTALL fts`로 받은 바이너리(`223b8cb9e6`)에서 직접 실행해 본 결과입니다.

- 트리거로 반영되는 것은 `INSERT`와 `DELETE`뿐입니다.
  `UPDATE`는 오류 없이 실행되지만 인덱스에 반영되지 않습니다(확인). 바꾸기 전 단어로는 계속 검색되고, 바꾼 뒤 단어로는 검색되지 않습니다.
  문서를 수정하려면 DELETE 후 INSERT로 해야 합니다.
- 문서 id 컬럼은 `NOT NULL` 또는 `PRIMARY KEY`여야 하고 값이 고유해야 합니다.
  이미 있는 id로 INSERT하면 `Constraint Error: Duplicate key ... violates unique constraint`로 실패합니다(확인).
- 영구 DB는 storage version `v2.0.0` 이상이어야 합니다. 스크립트는 `storage_compatibility_version = 'latest'`로 DB를 만듭니다.
- 트리거는 INSERT용 5개, DELETE용 5개이고 모두 `FOR EACH STATEMENT`입니다(확인). 문 하나의 비용은 위 [2.0.0 업데이트](#업데이트) 결과를 참고하세요.
- `cluster_terms = 1`이면 `terms`는 `__terms_storage` 테이블을 보여 주는 뷰가 됩니다(확인). 트리거로 추가된 행은 다시 정렬되지 않습니다(소스 기준).

## DuckDB 2.0.0 fts 설치 확인

2026-10-30에 확인했습니다.

- `pip install duckdb==2.0.0`은 성공합니다. PyPI의 `duckdb-extension-fts`는 여전히 1.5.5까지만 있습니다.
- 처음에는 이 환경의 네트워크 정책이 `extensions.duckdb.org`를 막아 `INSTALL fts`가 실패했습니다.
- 호스트를 허용한 뒤에도 기본 `INSTALL fts`는 `Could not establish connection`으로 실패했습니다.
  기본 저장소가 `http://`이고 DuckDB 내장 HTTP 클라이언트가 프록시 환경 변수를 쓰지 않기 때문입니다.
- [실행](#실행)에 적은 절차대로 `httpfs`를 파일로 설치하고, 프록시와 HTTPS 저장소를 지정하니 `INSTALL fts`가 성공했습니다.
  `duckdb_extensions()` 기준으로 `install_mode = REPOSITORY`, `installed_from = https://extensions.duckdb.org`, `extension_version = 223b8cb9e6`입니다. `allow_unsigned_extensions`는 `false`였습니다.
- 받은 바이너리의 버전 `223b8cb9e6`은 위에서 DuckDB v2.0.0이 고정한 커밋으로 적은 `2266e67d`와 다릅니다.
  이 차이가 무엇 때문인지는 확인하지 않았습니다. 측정은 이 바이너리로 했습니다.
