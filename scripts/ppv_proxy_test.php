<?php
const PPV_STREAMS = 'https://api.ppv.to/api/streams';
const POO_FETCH = 'https://pooembed.eu/fetch';
const POO_ORIGIN = 'https://pooembed.eu';
const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36';

function http_call(string $url, string $method = 'GET', ?string $body = null, array $headers = []): array {
    $ch = curl_init($url);
    $bag = [];
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true, CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_CONNECTTIMEOUT => 8, CURLOPT_TIMEOUT => 18,
        CURLOPT_USERAGENT => BROWSER_UA, CURLOPT_SSL_VERIFYPEER => false, CURLOPT_SSL_VERIFYHOST => false,
        CURLOPT_HEADERFUNCTION => static function ($ch, string $line) use (&$bag): int {
            $p = strpos($line, ':');
            if ($p !== false) $bag[strtolower(trim(substr($line, 0, $p)))] = trim(substr($line, $p + 1));
            return strlen($line);
        },
    ]);
    if ($method !== 'GET') { curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method); curl_setopt($ch, CURLOPT_POSTFIELDS, $body ?? ''); }
    if ($headers) curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    $raw = curl_exec($ch);
    $err = curl_error($ch);
    $code = curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
    curl_close($ch);
    if ($raw === false || $code < 200 || $code >= 300) throw new RuntimeException($err ?: "http {$code}");
    return [$raw, $bag, $code];
}

function enc_varint(int $n): string { $s = ''; while (true) { $b = $n & 0x7f; $n >>= 7; if ($n === 0) return $s . chr($b); $s .= chr($b | 0x80); } }
function get_varint(string $s, int &$i): int {
    $n = 0; $shift = 0; $len = strlen($s);
    while ($i < $len) { $b = ord($s[$i++]); $n |= ($b & 0x7f) << $shift; if ($b < 0x80) return $n; $shift += 7; }
    throw new RuntimeException('short varint');
}
function pb_put(int $field, string $value): string { return enc_varint(($field << 3) | 2) . enc_varint(strlen($value)) . $value; }
function pb_read(string $s): array {
    $i = 0; $out = []; $len = strlen($s);
    while ($i < $len) { $tag = get_varint($s, $i); $field = $tag >> 3; $wire = $tag & 7;
        if ($wire !== 2) throw new RuntimeException('bad wire');
        $n = get_varint($s, $i); if ($i + $n > $len) throw new RuntimeException('short field');
        $out[$field] = substr($s, (int)$i, (int)$n); $i += $n; }
    return $out;
}
function shift_payload(string $s): string { $o = ''; $len = strlen($s);
    for ($i = 0; $i < $len; $i++) { $c = ord($s[$i]); $o .= ($c >= 33 && $c <= 126) ? chr((($c - 33 + 71) % 94) + 33) : $s[$i]; }
    return $o; }
function u32(int $n): int { return $n & 0xffffffff; }
function le32(string $s, int $i): int { return ord($s[$i]) | (ord($s[$i + 1]) << 8) | (ord($s[$i + 2]) << 16) | (ord($s[$i + 3]) << 24); }
function put32(int $n): string { return pack('V', u32($n)); }
function rotl32(int $n, int $bits): int { $n = u32($n); return u32(($n << $bits) | ($n >> (32 - $bits))); }
function chacha_qr(array &$x, int $a, int $b, int $c, int $d): void {
    $x[$a] = u32($x[$a] + $x[$b]); $x[$d] = rotl32($x[$d] ^ $x[$a], 16);
    $x[$c] = u32($x[$c] + $x[$d]); $x[$b] = rotl32($x[$b] ^ $x[$c], 12);
    $x[$a] = u32($x[$a] + $x[$b]); $x[$d] = rotl32($x[$d] ^ $x[$a], 8);
    $x[$c] = u32($x[$c] + $x[$d]); $x[$b] = rotl32($x[$b] ^ $x[$c], 7);
}
function chacha_block(string $key, string $nonce, int $counter): string {
    $c = 'expand 32-byte k';
    $s = [le32($c,0),le32($c,4),le32($c,8),le32($c,12),le32($key,0),le32($key,4),le32($key,8),le32($key,12),le32($key,16),le32($key,20),le32($key,24),le32($key,28),u32($counter),le32($nonce,0),le32($nonce,4),le32($nonce,8)];
    $x = $s;
    for ($i = 0; $i < 10; $i++) { chacha_qr($x,0,4,8,12); chacha_qr($x,1,5,9,13); chacha_qr($x,2,6,10,14); chacha_qr($x,3,7,11,15); chacha_qr($x,0,5,10,15); chacha_qr($x,1,6,11,12); chacha_qr($x,2,7,8,13); chacha_qr($x,3,4,9,14); }
    $out = '';
    for ($i = 0; $i < 16; $i++) $out .= put32($x[$i] + $s[$i]);
    return $out;
}
function chacha_stream(string $key, string $nonce, int $len, int $counter): string {
    $out = '';
    while (strlen($out) < $len) { $out .= chacha_block($key, $nonce, $counter); $counter = u32($counter + 1); }
    return substr($out, 0, $len);
}
function pad16(string $s): string { $n = strlen($s) % 16; return $n === 0 ? '' : str_repeat("\0", 16 - $n); }
function le64(int $n): string { $lo = $n & 0xffffffff; $hi = intdiv($n, 4294967296); return pack('V2', $lo, $hi); }
function poly1305_mac(string $msg, string $key): string {
    $t0=le32($key,0);$t1=le32($key,4);$t2=le32($key,8);$t3=le32($key,12);
    $r0=$t0&0x3ffffff;$r1=(($t0>>26)|($t1<<6))&0x3ffff03;$r2=(($t1>>20)|($t2<<12))&0x3ffc0ff;$r3=(($t2>>14)|($t3<<18))&0x3f03fff;$r4=($t3>>8)&0x00fffff;
    $s1=$r1*5;$s2=$r2*5;$s3=$r3*5;$s4=$r4*5;
    $h0=$h1=$h2=$h3=$h4=0;$mask=0x3ffffff;
    $len=strlen($msg);
    for ($pos=0;$pos<$len;$pos+=16) { $n=min(16,$len-$pos);$block=substr($msg,$pos,$n);$hibit=1<<24;
        if ($n<16) { $block.="\x01".str_repeat("\0",15-$n);$hibit=0; }
        $t0=le32($block,0);$t1=le32($block,4);$t2=le32($block,8);$t3=le32($block,12);
        $h0+=$t0&$mask;$h1+=(($t0>>26)|($t1<<6))&$mask;$h2+=(($t1>>20)|($t2<<12))&$mask;$h3+=(($t2>>14)|($t3<<18))&$mask;$h4+=(($t3>>8)&0x00ffffff)|$hibit;
        $d0=$h0*$r0+$h1*$s4+$h2*$s3+$h3*$s2+$h4*$s1;
        $d1=$h0*$r1+$h1*$r0+$h2*$s4+$h3*$s3+$h4*$s2;
        $d2=$h0*$r2+$h1*$r1+$h2*$r0+$h3*$s4+$h4*$s3;
        $d3=$h0*$r3+$h1*$r2+$h2*$r1+$h3*$r0+$h4*$s4;
        $d4=$h0*$r4+$h1*$r3+$h2*$r2+$h3*$r1+$h4*$r0;
        $c=$d0>>26;$h0=$d0&$mask;$d1+=$c;$c=$d1>>26;$h1=$d1&$mask;$d2+=$c;
        $c=$d2>>26;$h2=$d2&$mask;$d3+=$c;$c=$d3>>26;$h3=$d3&$mask;$d4+=$c;
        $c=$d4>>26;$h4=$d4&$mask;$h0+=$c*5;$c=$h0>>26;$h0&=$mask;$h1+=$c; }
    $c=$h1>>26;$h1&=$mask;$h2+=$c;$c=$h2>>26;$h2&=$mask;$h3+=$c;$c=$h3>>26;$h3&=$mask;$h4+=$c;$c=$h4>>26;$h4&=$mask;$h0+=$c*5;$c=$h0>>26;$h0&=$mask;$h1+=$c;
    $g0=$h0+5;$c=$g0>>26;$g0&=$mask;$g1=$h1+$c;$c=$g1>>26;$g1&=$mask;$g2=$h2+$c;$c=$g2>>26;$g2&=$mask;$g3=$h3+$c;$c=$g3>>26;$g3&=$mask;$g4=$h4+$c-(1<<26);
    if ($g4>=0) { $h0=$g0;$h1=$g1;$h2=$g2;$h3=$g3;$h4=$g4; }
    $f0=(($h0|($h1<<26))&0xffffffff)+le32($key,16);$w0=$f0&0xffffffff;
    $f1=((($h1>>6)|($h2<<20))&0xffffffff)+le32($key,20)+($f0>>32);$w1=$f1&0xffffffff;
    $f2=((($h2>>12)|($h3<<14))&0xffffffff)+le32($key,24)+($f1>>32);$w2=$f2&0xffffffff;
    $f3=((($h3>>18)|($h4<<8))&0xffffffff)+le32($key,28)+($f2>>32);$w3=$f3&0xffffffff;
    return put32($w0).put32($w1).put32($w2).put32($w3);
}
function aead_input(string $aad, string $ciphertext): string { return $aad.pad16($aad).$ciphertext.pad16($ciphertext).le64(strlen($aad)).le64(strlen($ciphertext)); }
function bxor_stream(string $a, string $b): string { $out=''; $len=strlen($a); for ($i=0;$i<$len;$i++) $out.=chr(ord($a[$i])^ord($b[$i])); return $out; }
function open_payload(string $payload, string $island): string {
    $packed = base64_decode(shift_payload($payload), true);
    if ($packed === false || strlen($packed) < 28) throw new RuntimeException('bad payload');
    $nonce = substr($packed, 0, 12); $box = substr($packed, 12);
    $key = substr($island, 0, 32); $key = str_pad($key, 32, "\0");
    $tag = substr($box, -16); $ciphertext = substr($box, 0, -16);
    $poly_key = chacha_stream($key, $nonce, 32, 0);
    if (!hash_equals(poly1305_mac(aead_input('', $ciphertext), $poly_key), $tag)) throw new RuntimeException('bad tag');
    return bxor_stream($ciphertext, chacha_stream($key, $nonce, strlen($ciphertext), 1));
}
function fresh_url(string $slug): string {
    [$bin, $headers] = http_call(POO_FETCH, 'POST', pb_put(1, $slug), [
        'Content-Type: application/octet-stream', 'Origin: '.POO_ORIGIN,
        'Referer: '.POO_ORIGIN.'/embed/'.rawurlencode($slug), 'Accept: */*', 'User-Agent: '.BROWSER_UA,
    ]);
    $island = $headers['island'] ?? ''; if ($island === '') throw new RuntimeException('no island');
    $fields = pb_read($bin); if (!isset($fields[1])) throw new RuntimeException('no payload');
    return open_payload($fields[1], $island);
}
function hls_get(string $url, string $slug): string {
    [$text] = http_call($url, 'GET', null, [
        'Origin: '.POO_ORIGIN, 'Referer: '.POO_ORIGIN.'/embed/'.rawurlencode($slug),
        'Accept: */*', 'User-Agent: '.BROWSER_UA,
    ]);
    if (substr(ltrim($text), 0, 7) !== '#EXTM3U') throw new RuntimeException('not m3u8');
    return $text;
}

$id = $argv[1] ?? 'rally-tv';
echo "Testing: $id\n";

[$json] = http_call(PPV_STREAMS);
$data = json_decode($json, true);
$want = preg_replace('/^ppv-/', '', $id);
$slug = null;
foreach (($data['streams'] ?? []) as $cat) {
    foreach (($cat['streams'] ?? []) as $item) {
        $num = isset($item['id']) ? (string)$item['id'] : '';
        if ($num === $want || ('ppv-'.$num) === $id || (($item['uri_name'] ?? '') === $id)) {
            $slug = trim((string)($item['uri_name'] ?? ''));
            break 2;
        }
    }
}
if (!$slug) { echo "ERROR: stream not found\n"; exit(1); }
echo "Slug: $slug\n";

try {
    $source = fresh_url($slug);
    echo "CDN URL: $source\n";
    $text = hls_get($source, $slug);
    $lines = explode("\n", $text);
    echo "M3U8 OK: " . count($lines) . " lines\n";
    for ($i = 0; $i < min(15, count($lines)); $i++) echo "  {$lines[$i]}\n";
} catch (Throwable $e) {
    echo "FAILED: " . $e->getMessage() . "\n";
}
