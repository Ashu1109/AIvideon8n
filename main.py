# serverless_handler.py
import runpod, uuid, os, tempfile, subprocess, json, requests, shutil
from google.oauth2 import service_account
from googleapiclient.discovery import build

FFMPEG = "ffmpeg"                           # already in the RunPod base image
TMP_ROOT = "/tmp"

def download(url, out_path, timeout=120):
    r = requests.get(url, stream=True, timeout=timeout)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1 << 17):
            f.write(chunk)

def merge_videos(input_videos, narration, bg_audio, bg_vol, out_path):
    # Build filter_complex once to avoid re-encode loops
    inputs = "".join(f"-i {v} " for v in input_videos)
    if narration:   inputs += f"-i {narration} "
    if bg_audio:    inputs += f"-i {bg_audio} "
    filters = []
    if narration or bg_audio:
        streams = list(range(len(input_videos)))
        if narration: streams.append(len(input_videos))
        if bg_audio:  streams.append(len(input_videos) + (1 if narration else 0))
        amix = f"[{']['.join(map(str, streams))}]amix=inputs={len(streams)}:duration=longest:dropout_transition=2[aout]"
        filters.append(amix)
    cmd = f"{FFMPEG} {inputs} -filter_complex \"{';'.join(filters)}\" -map 0:v -map \"[aout]\" -c:v copy -c:a aac -y {out_path}"
    subprocess.check_call(cmd, shell=True)

def upload_to_drive(creds_json_str, local_path, drive_folder):
    creds = service_account.Credentials.from_service_account_info(json.loads(creds_json_str))
    service = build("drive", "v3", credentials=creds)
    file_meta = {"name": os.path.basename(local_path), "parents": [drive_folder]}
    media    = MediaFileUpload(local_path, resumable=True)
    return service.files().create(body=file_meta, media_body=media, fields="id,webViewLink").execute()

def handler(event):
    inp = event["input"]
    vids = inp["video_urls"]
    bg   = inp.get("background_audio_url")
    nar  = inp.get("narration_url")
    vol  = float(inp.get("background_volume", 0.3))
    gdrv = inp.get("upload_to_drive", False)

    job_dir = tempfile.mkdtemp(dir=TMP_ROOT, prefix="merge_")
    try:
        local_videos = []
        for u in vids:
            fname = os.path.join(job_dir, f"{uuid.uuid4()}.mp4")
            download(u, fname); local_videos.append(fname)

        nar_path = None
        if nar:
            nar_path = os.path.join(job_dir, "narration.mp3")
            download(nar, nar_path)

        bg_path = None
        if bg:
            bg_path = os.path.join(job_dir, "bg.mp3")
            download(bg, bg_path)

        out_file = os.path.join(job_dir, "merged.mp4")
        merge_videos(local_videos, nar_path, bg_path, vol, out_file)

        if gdrv:
            link = upload_to_drive(
                "service_account_json",
                out_file,
                "16tw9ifrTCLGC8RxioSh02_0TNngC4A_p"
            )
            return {"drive_file": link}

        # If not uploading, return base64 or presigned URL – simplest is to
        # read and base64-encode (≤ 100 MB works fine)
        import base64, mimetypes
        with open(out_file, "rb") as f:
            return {
                "filename": os.path.basename(out_file),
                "mimetype": mimetypes.guess_type(out_file)[0] or "video/mp4",
                "base64": base64.b64encode(f.read()).decode()
            }
    finally:
        shutil.rmtree(job_dir, ignore_errors=True)

runpod.serverless.start({"handler": handler})
