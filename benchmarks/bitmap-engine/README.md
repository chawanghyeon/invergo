# 비트맵 검색 엔진 실험 (Elasticsearch 대체 가능성)

이커머스 상품 검색을 겨냥한 실험용 엔진(Rust)과, 같은 데이터·같은 요청으로 Elasticsearch 9.2, Vespa 8.781과 비교한 결과입니다.

엔진의 핵심 아이디어는 세 가지입니다.

- 상품 docid를 인기 순서로 매깁니다. 그래서 인기순 top-k는 조건을 만족하는 문서를 앞에서부터 k개 찾으면 끝납니다(조기 종료).
- 카테고리, 재고, 가격 구간, 제목 단어를 모두 비트맵이나 posting으로 둡니다. 재고·가격 갱신은 비트 하나를 바꾸는 일이 됩니다.
- 카탈로그 전체를 노드 하나의 메모리에 둡니다(샤드 분배·병합 없음).

> 결론부터: 편향을 제거하고 비교하면 **필터·인기순 검색과 재고·가격 갱신은 우리 엔진이 앞서고, 필터가 붙은 BM25와 정확한 facet은 ES(1샤드)가 같거나 앞섭니다.**
> 초기에 주장했던 "7~14배"는 ES 과다 샤딩, 튜닝에 쓴 쿼리로 보고한 것 등 측정 오류가 섞인 수치였습니다([아래](#이전-주장-중-틀렸던-것)).

## 구성

| 경로 | 내용 |
|:--|:--|
| `engine/src/lib.rs` | 엔진 본체: 샤드, Roaring 비트맵, varint posting, leapfrog 교집합(조기 종료), 가격 버킷 순회, 일괄 갱신, 스냅샷, HTTP 서버 |
| `engine/src/realcat.rs` | 실제 데이터 적재, Lucene과 같은 BM25(SmallFloat norm, (k1+1) boost), MaxScore + block-max, facet(오프라인 후보 + 실시간 정확 집계), 쿼리 세트(튜닝용, held-out uniform/long-tail), 상관된 품절 모델 |
| `engine/src/main.rs` | 합성 데이터 벤치(`bench`, `concurrent`, `snapshot`, `primary`/`replica`, `rerank`, `serve`) |
| `engine/src/bin/prod.rs` | ES 호환 API(`/real/_search`, `/real/_bulk`) 서버: 디스크 문서 저장소(`_source`), bulk마다 fsync하는 WAL, `REPLAY=1`로 WAL 재생 |
| `engine/src/bin/es.rs` | 비교 클라이언트: ES/엔진/Vespa 적재, 교차 검증(`faircheck`), 편향 제거 벤치(`fair`), 품절 반영(`oos`) 등 |
| `engine/src/bin/real.rs` | 실제 데이터 단독 벤치(엔진 내부 측정) |
| `engine/src/bin/tv.rs` | Tantivy BM25 참고 측정 |
| `kotok/` | 한국어 형태소 분석(Lindera ko-dic) 속도와 사용자 사전 효과 |
| `vespa/app/` | Vespa 애플리케이션 패키지(스키마, `match-phase` 프로필, BM25 프로필) |
| `extract_amazon.py` | Amazon Reviews 2023 메타데이터(JSONL) → TSV 추출 |
| `bitmap_poc.py` | 초기 Python 개념 검증 |
| `results/` | 주요 측정 로그 원본 |

## 재현 방법

### 데이터

Hugging Face `McAuley-Lab/Amazon-Reviews-2023`의 `raw/meta_categories/meta_Electronics.jsonl`, `meta_Cell_Phones_and_Accessories.jsonl`을 스트리밍으로 받아 TSV로 만듭니다(합계 2,898,337건).

```bash
for c in Electronics Cell_Phones_and_Accessories; do
  curl -sSL "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw/meta_categories/meta_$c.jsonl" \
    | python3 -I extract_amazon.py > amazon/$c.tsv
done
cat amazon/Electronics.tsv amazon/Cell_Phones_and_Accessories.tsv > amazon/all.tsv
```

데이터셋에 없는 재고(`stock`, 90%)와 로켓배송(`rocket`, 40%) 필드는 docid로 결정되는 합성값입니다.

### 빌드

```bash
cd engine && RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### Elasticsearch 9.2.0

- root로 실행할 수 없으므로 별도 사용자(`es`)로 실행합니다. 설정: 단일 노드, 보안 끔, 힙 4 GB.
- 이 컨테이너는 디스크 사용률이 높게 계산되므로 워터마크를 절대값으로 지정해야 합니다.
  `cluster.routing.allocation.disk.watermark.{low,high,flood_stage}` = `3gb`, `2gb`, `1gb`.
- 적재: `SHARDS=1 es realload amazon/all.tsv`. 적재 후 `refresh_interval`을 1s로 되돌리고 force merge합니다.

### 편향 제거 비교

```bash
taskset -c 1-3 target/release/prod amazon/all.tsv proddata &      # 엔진 서버(포트 9400)
taskset -c 0 target/release/es faircheck amazon/all.tsv           # 세 시스템 결과 교차 검증
SPID=<서버 pid> taskset -c 0 target/release/es fair ours amazon/all.tsv
SPID=<ES pid>   taskset -c 0 target/release/es fair es   amazon/all.tsv
taskset -c 0 target/release/es oos es amazon/all.tsv              # 상관된 품절 상태 반영
```

HTTP 서버에는 `TCP_NODELAY`가 반드시 필요합니다. 빠지면 Nagle과 delayed ACK 때문에 응답마다 약 40 ms가 걸립니다.

## 결과: 편향을 제거한 비교

조건은 다음과 같습니다.

- 실제 상품 2,898,337건, 4 vCPU 한 대
- 서버는 코어 1~3, 클라이언트는 코어 0에 고정
- ES는 1샤드(이 크기 인덱스에 맞는 설정), 측정 전 예열
- 쿼리는 튜닝에 쓰지 않은 held-out 세트: 상품을 균일하게 뽑은 세트와 인기 하위 50%(롱테일)에서 뽑은 세트, 각 300개씩
- 모든 응답이 문서 원문(`_source`/summary)을 포함

### 결과 일치 (속도 비교의 전제)

| 항목 | ES (1샤드) | Vespa |
|:--|--:|--:|
| 필터 + 인기순 top-10 | 900/900 일치 | 900/900 일치 (`match-phase`) |
| BM25 (+재고, +카테고리&재고) | 점수 500/500 일치 (갱신 전) | top-10 겹침 91~95% (`weakAnd`, 근사) |
| 브랜드 facet top-20 | 정확값 300/300 일치 | `precision` 지정 시 300/300 일치 |

갱신 후에는 ES BM25 점수가 정확값과 달라집니다. 삭제 표시된 이전 버전 문서 656,350건이 병합 전까지 BM25 통계(N=3,554,528)에 남기 때문입니다.

### 성능

| 항목 | 우리 엔진 | ES 1샤드 | ES 4샤드 | Vespa |
|:--|--:|--:|--:|--:|
| 필터 검색 처리량 (16 클라이언트) | **10,700~11,500 QPS** (클라이언트 코어 99%, 서버 55~59%) | 3,460~3,700 QPS (서버 85%) | 1,940~2,170 QPS | 715~800 QPS |
| 필터 검색 단일 p50 | **0.39~0.51 ms** | 1.7~2.2 ms | 2.2~2.6 ms | 3.5~4.4 ms |
| 품절 갱신 후 필터 처리량 | **10,700~11,300 QPS** (변화 없음) | 2,240~2,340 QPS (-35%) | - | 750~800 QPS |
| BM25 + 필터 처리량 (held-out) | 734~857 QPS | **833~974 QPS** | 673~688 QPS | 331~445 QPS (근사) |
| BM25 + 필터 단일 p99 (held-out) | 28~31 ms | **13~16 ms** | 9~11 ms | 40~67 ms |
| 브랜드 facet (정확값) p50 | 2.2~2.7 ms | **1.3~1.5 ms** | 1.6~2.0 ms | 21~39 ms |
| 재고 일괄 갱신 (2,898,337건) | **1.0 s** | 148 s | - | 582 s |

- **우리 엔진의 필터 처리량은 클라이언트 병목입니다.** 클라이언트 코어 1개가 99% 바쁜 동안 서버는 3코어의 55~59%만 썼습니다. 서버 용량은 선형 추정으로 약 1.9~2.1만 QPS이고, ES 1샤드 용량은 약 4,100 QPS입니다. 따라서 필터 검색은 실측 약 3배, 용량 기준 약 5배입니다.
- **사전 계산 비용:** 290만 건 기준 전체 빌드 15~18초입니다. posting·속성 비트맵 7.4초, BM25 통계 6.8초(그중 block-max 3.1초), facet 후보 브랜드 4.3초입니다. 재고·가격 갱신에는 영향을 받지 않고, 제목·카탈로그가 바뀔 때 다시 만들어야 합니다. 신규 상품의 증분 추가는 아직 구현하지 않았습니다.
- **WAL 재생:** 재시작할 때 2,898,337건(143 MB)을 1.3초에 재생했고, 재생 후 결과가 ES와 300/300 일치했습니다.
- **RAM:** 엔진은 인덱스 전체를 메모리에 둡니다(상품당 약 96 B, 문서 원문은 디스크). ES는 디스크와 페이지 캐시로도 동작하므로, 같은 RAM 예산이라면 결과가 달라질 수 있습니다(측정하지 않음).

### 이전 주장 중 틀렸던 것

| 이전 주장 | 실제 | 원인 |
|:--|:--|:--|
| 필터 검색 7~14배 | 실측 약 3배, 용량 기준 약 5배 | ES를 4샤드로 나눔(1샤드보다 약 40% 느림), 클라이언트와 서버가 CPU를 공유 |
| BM25 처리량 2~3배 | held-out에서는 ES 1샤드가 약간 앞섬 | 튜닝에 쓴 쿼리 세트로 보고, ES 4샤드 |
| facet이 더 빠르고 정확함 | ES 1샤드가 정확하고 더 빠름 | ES 4샤드의 근사 집계와 비교 |
| ES 갱신 중 검색 p99 14.9 ms | 측정 무효 | 실제 데이터 인덱스의 `refresh_interval`을 `-1`로 둔 채 측정해서 ES 갱신이 검색에 반영되지 않았음 |

## 이번 목표: 필터 인식 BM25

필터가 붙은 BM25에서 진 원인은, 필터를 후보 문서마다 사후에 확인하는 구조였습니다. ES는 필터를 교집합 반복자로 함께 순회합니다.
이를 고친 뒤의 판정은 아래 [필터 인식 BM25 판정](#필터-인식-bm25-판정)에 기록합니다.

## 필터 인식 BM25 판정

### 바꾼 것

`realcat.rs`의 `bm25_shards_f`를 고쳤습니다.

- **필터를 순회 안으로:** 필터(카테고리, 재고, 가격 구간)를 후보마다 사후에 확인하지 않고, MaxScore/block-max 순회 안의 leapfrog 커서로 넣었습니다.
  - 후보 docid를 고르면, 필터마다 그 docid 이상인 다음 문서를 찾습니다.
  - 그 문서가 더 뒤에 있으면 단어 posting들을 거기까지 `advance_to`로 건너뜁니다.
  - 필터가 선택적일수록 이 건너뛰기가 커져서, 필터 비트맵을 드라이버로 쓰는 것과 같은 효과가 납니다. 그래서 별도 경로는 만들지 않았습니다.
- **후보당 오버헤드 제거:** 구간 상한 버퍼를 재사용하고, 같은 128-docid 구간 안에서는 block-max 상한을 다시 계산하지 않습니다.
- **임계값은 바꾸지 않았습니다.**
- **개발과 보고 분리:** 개발과 튜닝은 dev 세트(seed 400/500)로만 했습니다. 판정은 **한 번도 보지 않은 새 held-out 세트(seed 600/700, 유형당 500개)** 로, 측정 전에 조건을 정해 두고 했습니다.

### 정확성

| 확인 | 결과 |
|:--|:--|
| 전수 계산(`bm25_exhaustive_f`)과 비교 (dev 1,000개, id와 점수) | **1,000/1,000 일치** |
| 새로 적재한 ES 1샤드와 top-10 점수 (판정용 새 세트, 갱신 전) | **2,000/2,000 일치** |
| 기존 held-out 세트에서 ES와 비교 (필터 900, BM25 500, facet 300) | 전부 일치 |

### 성능 (판정용 새 세트, 2회 반복, 측정 순서를 바꿔 가며)

조건은 서버 코어 1~3, 클라이언트 코어 0, ES 1샤드에 `refresh_interval` 1s, 두 번 예열입니다. 두 서버 모두 3코어의 97~99%를 썼고, 클라이언트 코어는 23~32%만 써서 병목이 아니었습니다.

| 세트 / 쿼리 | 우리 엔진 처리량 (1회 / 2회) | ES 1샤드 처리량 | 우리 단일 p99 | ES 단일 p99 |
|:--|--:|--:|--:|--:|
| uniform, BM25 + 재고 | **2,017 / 2,015 QPS** | 1,075 / 990 | **7.9 / 6.5 ms** | 15.6 / 14.2 ms |
| uniform, BM25 + 카테고리 + 재고 | **1,754 / 1,833 QPS** | 1,343 / 1,371 | **8.4 / 10.9 ms** | 11.4 / 11.0 ms |
| long-tail, BM25 + 재고 | **2,059 / 2,124 QPS** | 972 / 986 | **9.9 / 6.7 ms** | 18.8 / 18.6 ms |
| long-tail, BM25 + 카테고리 + 재고 | **1,907 / 1,933 QPS** | 1,420 / 1,451 | **9.0 / 8.9 ms** | 9.6 / 9.8 ms |

16 클라이언트 p99는 우리 엔진 17~21 ms, ES 28~45 ms였습니다.

### 판정: 성공 기준 충족 → ES 전면 대체 방향 유지

8개 칸(세트 2 × 쿼리 2 × 반복 2) 모두에서 16 클라이언트 처리량과 단일 p99가 ES 1샤드 이상입니다. 다만 차이의 크기는 쿼리 유형에 따라 다릅니다.

- **BM25 + 재고:** 처리량 약 1.9~2.2배, 단일 p99 약 2~2.8배 낮습니다. 확실한 우위입니다.
- **BM25 + 카테고리 + 재고:** 처리량은 1.30~1.34배로 앞섭니다. 하지만 단일 p99는 4칸 중 3칸이 거의 동률입니다(8.9 대 9.6, 10.9 대 11.0, 9.0 대 9.8 ms). 이 유형의 단일 지연은 "앞선다"보다 "같은 수준"이 정확합니다.

### 판정 과정에서 드러난 것 (불리한 결과 포함)

- **최적화 전에는 기준을 못 넘었습니다.** 필터 커서만 넣은 상태에서 기존 held-out 세트(seed 200/300)를 2회 측정했더니, 처리량은 4칸 모두 앞섰습니다. 하지만 2회차 long-tail BM25 + 카테고리 + 재고의 단일 p99가 **10.2 ms 대 ES 8.8 ms로 기준 미달**이었습니다(`results/fair_bm25_split_rep2.log`). 후보당 오버헤드를 dev 세트로만 줄인 뒤, 새 세트로 다시 판정했습니다.
- **같은 쿼리여도 ES 측정값이 실행마다 10~20% 흔들렸습니다.** 예를 들어 같은 세트의 BM25 + 필터가 833~974 QPS였다가 1,031~1,188 QPS로 나왔습니다. 그래서 2회 반복하고 순서를 바꿨습니다.
- **ES가 여전히 앞서는 영역:** 정확한 facet입니다. ES 1샤드는 p50 1.3~1.9 ms, 우리 엔진은 2.0~2.7 ms입니다. 이번 목표(BM25)와는 별개로 남은 격차입니다.

### 이 판정의 한계

- 단일 노드, 290만 건, 4 vCPU에서 측정했습니다. 5억 건 규모와 다중 노드는 검증하지 않았습니다.
- ES 설정은 문서화된 범위 안에서 했습니다(1샤드, force merge, 인덱스 정렬). Lucene 전문가 수준의 튜닝을 거친 결과는 아닙니다.
- 우리 엔진은 ES 기능 중 일부만 지원합니다. 비교는 두 시스템이 같은 결과를 내는 쿼리에서만 했습니다.
- 인덱스 전체를 RAM에 두는 비용(상품당 약 96 B)은 위 수치에 반영되지 않았습니다.
