//! Korean product-title tokenization with Lindera (ko-dic): without vs with an e-commerce user dictionary.
use lindera::dictionary::{load_dictionary, load_user_dictionary, Metadata};
use lindera::mode::Mode;
use lindera::segmenter::Segmenter;
use std::borrow::Cow;
use std::time::Instant;

const BRANDS: &[&str] = &["삼성전자", "엘지전자", "애플", "샤오미", "나이키", "아디다스", "쿠쿠", "필립스", "다이슨", "로지텍"];
const NOUNS: &[&str] = &["무선이어폰", "블루투스스피커", "노트북가방", "기계식키보드", "공기청정기", "전기밥솥", "운동화", "런닝화",
                         "무선청소기", "게이밍마우스", "스마트워치", "보조배터리", "고속충전기", "휴대폰케이스", "강화유리필름"];
const ADJ: &[&str] = &["초경량", "대용량", "프리미엄", "가성비", "정품", "국내정발", "당일발송", "신형", "방수", "저소음"];
const SPEC: &[&str] = &["블랙", "화이트", "그레이", "270mm", "65W", "10000mAh", "2024년형", "1+1", "C타입", "15.6인치"];

fn pick<'a>(v: &[&'a str], i: u64, f: u64) -> &'a str {
    let x = (i.wrapping_mul(0x9E3779B97F4A7C15) ^ f.wrapping_mul(0xBF58476D1CE4E5B9)).rotate_left(17);
    v[(x % v.len() as u64) as usize]
}
fn toks(seg: &Segmenter, s: &str) -> Vec<String> {
    seg.segment(Cow::Borrowed(s)).unwrap().iter().map(|t| t.surface.to_string()).collect()
}

fn main() {
    let meta: Metadata = serde_json::from_reader(std::fs::File::open("ko_metadata.json").unwrap()).unwrap();
    let plain = Segmenter::new(Mode::Normal, load_dictionary("embedded://ko-dic").unwrap(), None);
    let user = load_user_dictionary("user_dict.csv", &meta).unwrap();
    let dict = Segmenter::new(Mode::Normal, load_dictionary("embedded://ko-dic").unwrap(), Some(user));
    let protected: Vec<String> = std::fs::read_to_string("user_dict.csv").unwrap().lines().map(|l| l.split(',').next().unwrap().to_string()).collect();

    let titles: Vec<String> = (0..200_000u64)
        .map(|i| format!("{} {} {}{} {} {}", pick(BRANDS, i, 1), pick(ADJ, i, 2), pick(NOUNS, i, 3), pick(&["", " 세트", "용"], i, 4), pick(SPEC, i, 5), pick(NOUNS, i, 6)))
        .collect();
    for s in titles.iter().take(5) {
        println!("  {s}\n    without: {}\n    with   : {}", toks(&plain, s).join(" | "), toks(&dict, s).join(" | "));
    }
    for (name, seg) in [("without user dict", &plain), ("with user dict   ", &dict)] {
        // how often a dictionary term that occurs in the title survives as one token
        let (mut occ, mut kept) = (0, 0);
        for s in titles.iter().take(20_000) {
            let t = toks(seg, s);
            for p in &protected {
                if s.contains(p.as_str()) {
                    occ += 1;
                    kept += t.iter().any(|x| x == p) as u32;
                }
            }
        }
        let t0 = Instant::now();
        let mut n = 0usize;
        for s in &titles {
            n += seg.segment(Cow::Borrowed(s.as_str())).unwrap().len();
        }
        let secs = t0.elapsed().as_secs_f64();
        println!("{name}: dictionary terms kept whole {kept}/{occ} ({:.1}%), {:.1} tokens/title, {:.0} titles/s single thread",
                 100.0 * kept as f64 / occ as f64, n as f64 / titles.len() as f64, titles.len() as f64 / secs);
    }
}
