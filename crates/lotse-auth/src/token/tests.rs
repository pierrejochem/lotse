use super::*;

const DAY_MS: u64 = 24 * 60 * 60 * 1000;

fn sample() -> Token {
    let mut secret = [0u8; 32];
    for (i, b) in secret.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(37).wrapping_add(11);
    }
    Token::new(0x0123_4567_89ab_cdef, secret)
}

#[test]
fn lifetimes_are_the_five_steps() {
    let days: Vec<u64> = (1..=5).map(|c| Lifetime::from_code(c).unwrap().millis() / DAY_MS).collect();
    assert_eq!(days, [7, 30, 90, 180, 365]);
    for c in 1..=5 {
        assert_eq!(Lifetime::from_code(c).unwrap().code(), c);
    }
    assert_eq!(Lifetime::from_code(0), None);
    assert_eq!(Lifetime::from_code(6), None);
}

#[test]
fn a_token_round_trips_through_its_text() {
    let t = sample();
    let text = t.text();
    assert_eq!(Token::parse(&text), Some(t));
    let rest = text.strip_prefix("lotse_tok_").expect("prefix");
    let (id, secret) = rest.split_once('_').expect("separator");
    assert_eq!(id, "0123456789abcdef");
    assert_eq!(secret.len(), 43);
}

#[test]
fn a_pasted_token_with_whitespace_parses() {
    let t = sample();
    assert_eq!(Token::parse(&format!("  {}\n", t.text())), Some(t));
}

#[test]
fn malformed_tokens_do_not_parse() {
    let text = sample().text();
    let secret = &text[text.len() - 43..];
    let bad = [
        text.replacen("lotse_tok_", "lotse_key_", 1),
        format!("lotse_tok_{}_{}", "0123456789abcde", secret),
        format!("lotse_tok_0123456789abcdef_{}", &secret[..42]),
        format!("lotse_tok_0123456789abcdef_{}!", &secret[..42]),
        // 'B' ends in non-zero spare bits: not the canonical text of any secret.
        format!("lotse_tok_0123456789abcdef_{}B", &secret[..42]),
        format!("lotse_tok_0123456789ABCDEF_{secret}"),
        format!("lotse_tok_0123456789abcdef_{secret}x"),
        format!("lotse_tok_0123456789abcdef{secret}"),
        String::new(),
    ];
    for b in bad {
        assert_eq!(Token::parse(&b), None, "{b}");
    }
}

#[test]
fn only_the_right_secret_matches() {
    let t = sample();
    let stored = t.hash();
    assert!(matches(&stored, &t));
    let mut other = t.secret;
    other[31] ^= 1;
    assert!(!matches(&stored, &Token::new(t.id, other)));
}

#[test]
fn debug_does_not_show_the_secret() {
    let t = sample();
    let shown = format!("{t:?}");
    assert!(!shown.contains(&t.text()[t.text().len() - 43..]));
    assert!(!shown.contains(&format!("{:?}", t.secret)));
}
