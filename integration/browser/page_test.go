package browser

// conformancePage is the deterministic page the lane drives. Everything it
// asserts is readable through computer.eval, because a conformance run has to
// check what the page actually became rather than what a picture looks like.
//
// The iframe's field reports through postMessage instead of being read across
// the frame boundary. A file:// document is an opaque origin, so reaching into
// a child frame would need --allow-file-access-from-files; posting a message
// needs nothing, and the assertion it supports is the same either way.
const conformancePage = `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Remount browser conformance</title>
<style>
  body { margin: 0; background: #ffffff; color: #111111;
         font: 16px/1.4 "Liberation Sans", "DejaVu Sans", sans-serif; }
  #panel { padding: 16px; }
  #banner { width: 320px; height: 40px; line-height: 40px; background: #dddddd;
            text-align: center; }
  #go { display: block; width: 320px; height: 48px; margin: 12px 0; font-size: 16px; }
  #field { display: block; width: 320px; height: 36px; margin: 12px 0;
           font-size: 16px; box-sizing: border-box; }
  #note { display: block; width: 320px; height: 60px; margin: 12px 0; padding: 4px;
          border: 1px solid #888888; box-sizing: border-box; }
  #frame { display: block; width: 320px; height: 72px; margin: 12px 0;
           border: 1px solid #888888; }
  #dl { display: block; width: 320px; height: 36px; line-height: 36px; margin: 12px 0;
        background: #eeeeee; text-align: center; color: #111111; }
</style>
</head>
<body>
<div id="panel">
  <div id="banner">idle</div>
  <button id="go">Press me</button>
  <input id="field" type="text" value="">
  <div id="note" contenteditable="true"></div>
  <iframe id="frame"></iframe>
  <a id="dl" download="remount-conformance.txt">Download</a>
</div>
<script>
window.__clicks = 0;
window.__iframeValue = "";
document.getElementById("go").addEventListener("click", function () {
  window.__clicks += 1;
  var banner = document.getElementById("banner");
  banner.textContent = "clicked " + window.__clicks;
  banner.style.background = "#00aa00";
});
window.addEventListener("message", function (event) {
  if (event.data && typeof event.data.iframeValue === "string") {
    window.__iframeValue = event.data.iframeValue;
  }
});
document.getElementById("frame").srcdoc =
  "<!doctype html><body style='margin:0;font:16px sans-serif'>" +
  "<input id='inner' style='width:300px;height:32px;font-size:16px;box-sizing:border-box'>" +
  "<" + "script>document.getElementById('inner').addEventListener('input', function (e) {" +
  "parent.postMessage({iframeValue: e.target.value}, '*');});<" + "/script></body>";
document.getElementById("dl").href = URL.createObjectURL(
  new Blob(["remount browser conformance download\n"], {type: "text/plain"}));
</script>
</body>
</html>
`

// downloadBody is what the page's download link carries, byte for byte.
const downloadBody = "remount browser conformance download\n"

// downloadName is the download attribute the page declares.
const downloadName = "remount-conformance.txt"
