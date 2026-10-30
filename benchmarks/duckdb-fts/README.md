# DuckDB FTS 벤치마크

DuckDB `fts` 확장(BM25)만으로 경량 검색 서버를 만들 수 있는지 판단하기 위한 벤치마크입니다.
합성 코퍼스(Zipf 분포)를 만들어 아래 항목을 측정하고 결과를 JSON으로 저장합니다.

> 현재 커밋된 결과는 **DuckDB 1.5.5 + 정적 인덱스** 기준입니다.
> 이 환경에서는 DuckDB 2.0.0용 `fts` 바이너리를 받을 수 없어 2.0.0은 아직 측정하지 못했습니다
> ([아래](#duckdb-200-측정-상태) 참고).

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
   2.0.0에서 따로 측정해야 합니다.

## 이전 답변 검증

| 이전 답변의 내용 | 확인 결과 |
|:--|:--|
| "외부 패키지 다운로드가 차단되어 DuckDB 2.0.0 설치 실패" | 이 환경에서는 `pip install duckdb==2.0.0`이 성공합니다(PyPI에 2.0.0 존재, `source_id d54cbbe3df` = 태그 `v2.0.0`). 막힌 것은 fts 확장을 받는 `extensions.duckdb.org`입니다. pip 설치에 성공해도 `LOAD fts`에서 실패합니다. |
| "공식 2.0 문서에서 incremental FTS와 `match_bm25` 지원 확인" | 맞습니다. DuckDB v2.0.0이 고정한 duckdb-fts 커밋 `2266e67d`의 README와 소스에 `create_fts_index(..., incremental = 0)` 옵션과 `match_bm25`가 있습니다. 제약은 아래에 정리했습니다. |
| "벤치마크 코드 작성, 문법 검사 완료" | 전달된 내용에는 설명만 있고 코드는 이 저장소에 없습니다. 이 디렉터리의 스크립트를 새로 작성했고 실제로 실행했습니다. |
| 실행 방법 `pip install duckdb==2.0.0` 후 실행 | 이것만으로는 부족합니다. fts 확장은 pip 패키지에 들어 있지 않고 런타임에 `extensions.duckdb.org`에서 받아야 합니다. |
| "실제 쿼리 실행 및 성능 측정 미완료" | 1.5.5와 공식 서명 fts로 10만 건과 100만 건을 실측했습니다(위). 2.0.0은 아직 측정하지 못했습니다. |
| "10만 → 100만 → 1,000만 건 확장 가능" | 스크립트는 `--rows`로 늘릴 수 있습니다. 다만 1M에서 이미 프로세스 피크 RSS가 4.8 GB였으므로, 1,000만 건은 16 GB 머신에서 메모리가 부족할 가능성이 높습니다(미측정). |

### 2.0 incremental FTS의 제약 (fts `2266e67d` README와 소스 기준)

- 트리거로 반영되는 것은 `INSERT`와 `DELETE`뿐입니다. 소스에 `UPDATE` 트리거가 없으므로 문서 수정은 DELETE 후 INSERT로 해야 할 것으로 보입니다(미검증).
- 문서 id 컬럼은 `NOT NULL` 또는 `PRIMARY KEY`여야 하고 값이 고유해야 합니다.
- 영구 DB는 storage version `v2.0.0` 이상이어야 합니다. 스크립트는 `storage_compatibility_version = 'latest'`로 DB를 만듭니다.
- 트리거는 `FOR EACH STATEMENT`입니다. INSERT 문 하나마다 토큰화와 dict/terms/stats 갱신 문이 여러 개 실행되므로 배치 크기가 성능에 큰 영향을 줄 것으로 보입니다.
- `cluster_terms = 1`이어도 트리거로 추가된 행은 다시 정렬되지 않습니다.

## DuckDB 2.0.0 측정 상태

- `pip install duckdb==2.0.0`은 성공합니다.
- `LOAD fts`는 `extensions.duckdb.org` 접속 실패로 안 됩니다.
- PyPI의 `duckdb-extension-fts`는 1.5.5까지만 있습니다.
- 확장 바이너리는 DuckDB 버전과 정확히 맞아야 하므로 1.5.5용 파일은 2.0.0에서 쓸 수 없습니다.

2.0.0을 측정하는 방법은 두 가지입니다.

1. `extensions.duckdb.org`에 접속할 수 있는 환경에서 실행합니다(가장 간단합니다).
   ```bash
   python duckdb_fts_benchmark.py --rows 1000000 --append 100000 --incremental on \
     --out results/duckdb-2.0.0_incremental_1m.json
   python duckdb_fts_benchmark.py --rows 1000000 --append 100000 --incremental on --cluster-terms \
     --out results/duckdb-2.0.0_incremental_clustered_1m.json
   python duckdb_fts_benchmark.py --rows 1000000 --append 100000 --incremental off \
     --out results/duckdb-2.0.0_static_1m.json
   ```
2. DuckDB `v2.0.0` 소스와 fts `2266e67d`(DuckDB 저장소의 `.github/patches/extensions/fts/` 패치 3개 적용)로 확장을 빌드합니다.
   이렇게 빌드한 확장은 서명이 없으므로 `--fts-extension <빌드 결과> --allow-unsigned`로 실행합니다.
