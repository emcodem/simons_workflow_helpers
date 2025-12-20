import sys
import unittest
from pathlib import Path
import tempfile
import subprocess
import argparse

class TestGPUEncodingCmd(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.original_cmd = (
            '"\\\\seriouslto\\FFastrans_Working_Version\\FFAStrans-Public-1.4.2\\processors\\FFMpeg\\x64\\ffmpeg.exe"   '
            '-stats_period 1.7 -hide_banner  -reinit_filter 0 -i "\\\\seriouslto\\FFastrans_Working_Version\\Cache\\20251210-0152-0779-54d9-e1e50b926c2a\\20251210-1801-3257-7e24-7ce9ce672c55\\1921025-16-48~251210201255569~32612~20251102-0911-1744-7074-59b08db0fc6f~enc_av_mp4.avs" '
            '-shortest -map_metadata -1 -filter_complex [0:0]sidedata=delete,metadata=delete,setpts=PTS-STARTPTS,setparams=range=tv:color_primaries=bt709:color_trc=bt709:colorspace=bt709,fps=25,format=yuv420p10,setparams=field_mode=prog:range=tv:color_primaries=bt709:color_trc=bt709:colorspace=bt709,setsar=r=1:max=1[vstr1] '
            '-map [vstr1] -filter_complex "[0:1]asidedata=delete,ametadata=delete,pan=1|c0=c0[a1],[0:1]asidedata=delete,ametadata=delete,pan=1|c0=c1[a2],[a1][a2]amerge=2[astr1]" '
            '-map "[astr1]" -c:a:0 aac -b:a:0 356k -ar:a:0 48000  -profile:v high10 -preset medium -movflags faststart -c:v libx264 -b:v 60266k -r 25 '
            '-color_primaries bt709 -color_trc bt709 -colorspace bt709 -color_range tv -field_order progressive  -brand mp42  -timecode 00:00:00:00   '
            '-max_muxing_queue_size 700 -map_metadata -1 -metadata creation_time=now  -y "\\\\seriouslto\\FFastrans_Working_Version\\Cache\\20251210-0152-0779-54d9-e1e50b926c2a\\20251210-1801-3257-7e24-7ce9ce672c55\\1921025-16-48~251210201303046~32612~20251102-0911-1744-7074-59b08db0fc6f~enc_av_mp4.mp4"'
        )
        self.temp_dir = tempfile.TemporaryDirectory()
    
    def tearDown(self):
        """Clean up temporary files"""
        self.temp_dir.cleanup()
    
    def run_script(self, cmd_file_path, additional_options="-preset p5 -rc vbr_hq -cq 22 -b:v 0 -g 50 -bf 3", 
                   bmx_cmd_file=None, insert_hwupload_cuda=False, replace_output=None, assume_source_fps=None):
        """Execute the gpu_encoding_cmd script with given arguments"""
        cmd = [
            sys.executable,
            "gpu_encoding_cmd.py",
            cmd_file_path,
            "--test",
            "--additional-options", additional_options
        ]
        
        if bmx_cmd_file:
            cmd.extend(["--bmx_cmd_file", bmx_cmd_file])
        
        if insert_hwupload_cuda:
            cmd.append("--insert_hwupload_cuda")
        
        if replace_output:
            cmd.extend(["--replace_output", replace_output])
        
        if assume_source_fps:
            cmd.extend(["--assume_source_fps", assume_source_fps])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result
    
    def test_replace_libx264_with_h264_nvenc(self):
        """Test that libx264 is replaced with h264_nvenc"""
        cmd_file = Path(self.temp_dir.name) / "cmd.txt"
        cmd_file.write_text(self.original_cmd)
        
        result = self.run_script(str(cmd_file))
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        self.assertIn("h264_nvenc", result.stdout)
        # Find the modified command (after the last "====") and check it doesn't contain libx264
        parts = result.stdout.split("=============================\n")
        if len(parts) >= 2:
            modified_cmd = parts[-1]
            self.assertNotIn("-c:v libx264", modified_cmd)
        # Verify metadata creation_time is preserved
        self.assertIn("-metadata creation_time=now", result.stdout)
    
    def test_preset_removed(self):
        """Test that -preset medium is removed from the command"""
        cmd_file = Path(self.temp_dir.name) / "cmd.txt"
        cmd_file.write_text(self.original_cmd)
        
        result = self.run_script(str(cmd_file), additional_options="-preset p5 -rc vbr_hq -cq 22 -b:v 0 -g 50 -bf 3")
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        # Check that the modified command (last section after diffs) doesn't have the old preset
        parts = result.stdout.split("=============================\n")
        if len(parts) >= 2:
            modified_cmd = parts[-1]
            self.assertNotIn("-preset medium", modified_cmd)
            self.assertIn("-preset p5", modified_cmd)
        # Verify metadata creation_time is preserved
        self.assertIn("-metadata creation_time=now", result.stdout)
    
    def test_hwupload_cuda_insertion(self):
        """Test that hwupload_cuda is inserted when flag is set"""
        cmd_file = Path(self.temp_dir.name) / "cmd.txt"
        cmd_file.write_text(self.original_cmd)
        
        result = self.run_script(str(cmd_file), insert_hwupload_cuda=True)
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        self.assertIn("hwupload_cuda", result.stdout)
        # Verify metadata creation_time is preserved
        self.assertIn("-metadata creation_time=now", result.stdout)
    
    def test_additional_options_appended(self):
        """Test that additional options are appended after h264_nvenc"""
        cmd_file = Path(self.temp_dir.name) / "cmd.txt"
        cmd_file.write_text(self.original_cmd)
        
        result = self.run_script(str(cmd_file), additional_options="-preset p4 -rc vbr_hq -cq 28")
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        self.assertIn("h264_nvenc", result.stdout)
        self.assertIn("-rc vbr_hq", result.stdout)
        # Verify metadata creation_time is preserved
        self.assertIn("-metadata creation_time=now", result.stdout)
    
    def test_script_returns_zero_on_test_mode(self):
        """Test that the script returns exit code 0 in test mode"""
        cmd_file = Path(self.temp_dir.name) / "cmd.txt"
        cmd_file.write_text(self.original_cmd)
        
        result = self.run_script(str(cmd_file))
        self.assertEqual(result.returncode, 0)
    
    def test_replace_output(self):
        """Test that output file is replaced when replace_output is set"""
        cmd_file = Path(self.temp_dir.name) / "cmd.txt"
        cmd_file.write_text(self.original_cmd)
        
        new_output_path = "C:/new/output/path.mp4"
        result = self.run_script(str(cmd_file), replace_output=new_output_path)
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        # Check that the new output path is in the modified command
        self.assertIn(new_output_path, result.stdout)
        # Verify metadata creation_time is preserved
        self.assertIn("-metadata creation_time=now", result.stdout)
    
    def test_prores_encoding_command(self):
        """Test with ProRes encoding command (non-libx264 codec)"""
        prores_cmd = (
            '"C:\\FFAStrans-Public-1.4.2\\processors\\FFMpeg\\x64\\ffmpeg.exe"  -analyzeduration 33554432  -stats_period 1.7 -hide_banner  -reinit_filter 0 -i "C:\\temp\\NoisyStage.mov" '
            '-shortest -map_metadata -1 -filter_complex [0:1]sidedata=delete,metadata=delete,setpts=PTS-STARTPTS,setparams=range=tv:color_primaries=bt709:color_trc=bt709:colorspace=bt709,fps=25,setparams=field_mode=tff:range=tv:color_primaries=bt709:color_trc=bt709:colorspace=bt709,setsar=r=1:max=1[vstr1] '
            '-map [vstr1] -filter_complex "[0:0]asidedata=delete,ametadata=delete,pan=1|c0=c0[a1],[0:0]asidedata=delete,ametadata=delete,pan=1|c0=c1[a2],[a1][a2]amerge=2[astr1]" '
            '-map "[astr1]" -c:a:0 pcm_s24le -ar:a:0 48000 -disposition:a default   -field_order tt -flags +ildct+ilme  '
            '-c:v prores_ks -profile:v 3 -vtag apch -quant_mat auto -aspect 16:9 -color_primaries bt709 -color_trc bt709 -colorspace bt709 -color_range tv  '
            '-timecode 00:00:00:00 -r 25 -pix_fmt yuv422p10 -max_muxing_queue_size 700 -map_metadata -1 -metadata creation_time=now  -y '
            '"c:\\.ffastrans_work_root\\20251119-1403-3517-5408-8e9a27b353a5\\20251210-2209-2884-7192-f6415343278e\\1-18-18~251210220931300~39956~20251101-1233-3640-436e-067cd1bad710~enc_av_prores.mov"'
        )
        cmd_file = Path(self.temp_dir.name) / "prores_cmd.txt"
        cmd_file.write_text(prores_cmd)
        
        result = self.run_script(str(cmd_file))
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        # Verify the command was processed without errors
        # Since this doesn't use libx264, it won't have h264_nvenc replacement
        # Just verify the original codec is preserved
        self.assertIn("prores_ks", result.stdout)
        # Verify metadata creation_time is preserved
        self.assertIn("-metadata creation_time=now", result.stdout)
    
    def test_prores_with_replace_output(self):
        """Test ProRes encoding command with replace_output parameter"""
        prores_cmd = (
            '"C:\\FFAStrans-Public-1.4.2\\processors\\FFMpeg\\x64\\ffmpeg.exe"  -analyzeduration 33554432  -stats_period 1.7 -hide_banner  -reinit_filter 0 -i "C:\\temp\\NoisyStage.mov" '
            '-shortest -map_metadata -1 -filter_complex [0:1]sidedata=delete,metadata=delete,setpts=PTS-STARTPTS,setparams=range=tv:color_primaries=bt709:color_trc=bt709:colorspace=bt709,fps=25,setparams=field_mode=tff:range=tv:color_primaries=bt709:color_trc=bt709:colorspace=bt709,setsar=r=1:max=1[vstr1] '
            '-map [vstr1] -filter_complex "[0:0]asidedata=delete,ametadata=delete,pan=1|c0=c0[a1],[0:0]asidedata=delete,ametadata=delete,pan=1|c0=c1[a2],[a1][a2]amerge=2[astr1]" '
            '-map "[astr1]" -c:a:0 pcm_s24le -ar:a:0 48000 -disposition:a default   -field_order tt -flags +ildct+ilme  '
            '-c:v prores_ks -profile:v 3 -vtag apch -quant_mat auto -aspect 16:9 -color_primaries bt709 -color_trc bt709 -colorspace bt709 -color_range tv  '
            '-timecode 00:00:00:00 -r 25 -pix_fmt yuv422p10 -max_muxing_queue_size 700 -map_metadata -1 -metadata creation_time=now  -y '
            '"c:\\.ffastrans_work_root\\20251119-1403-3517-5408-8e9a27b353a5\\20251210-2209-2884-7192-f6415343278e\\1-18-18~251210220931300~39956~20251101-1233-3640-436e-067cd1bad710~enc_av_prores.mov"'
        )
        cmd_file = Path(self.temp_dir.name) / "prores_cmd_replace.txt"
        cmd_file.write_text(prores_cmd)
        
        new_output_path = "C:\\temp\\NoisyStage_offspeed_25.mov"
        result = self.run_script(str(cmd_file), replace_output=new_output_path)
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        # Verify the output path was replaced
        self.assertIn(new_output_path, result.stdout)
        # Verify the original codec is preserved
        self.assertIn("prores_ks", result.stdout)
        # Verify metadata creation_time is preserved
        self.assertIn("-metadata creation_time=now", result.stdout)
    
    def test_assume_source_fps(self):
        """Test that assume_source_fps inserts -r flag before -i"""
        cmd_file = Path(self.temp_dir.name) / "cmd_fps.txt"
        cmd_file.write_text(self.original_cmd)
        
        result = self.run_script(str(cmd_file), assume_source_fps="25")
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        # Verify the -r flag with fps value was inserted before -i
        self.assertIn("-r 25 -i", result.stdout)
        # Verify metadata creation_time is preserved
        self.assertIn("-metadata creation_time=now", result.stdout)
    
    def test_bmx_cmd_file(self):
        """Test bmx_cmd_file replacement in FFmpeg command with bmxtranswrap pipe"""
        # Create FFmpeg command with bmxtranswrap pipe
        ffmpeg_cmd_with_bmx = (
            '"\\\\seriouslto\\FFastrans_Working_Version\\FFAStrans-Public-1.4.2\\processors\\FFMpeg\\x64\\ffmpeg.exe"  -analyzeduration 33554432  -stats_period 1.7 -hide_banner  -reinit_filter 0 -i '
            '"\\\\192.168.2.11\\2025_01_Dump\\Simon_Ffastrans_Dev\\2025\\02_Jobs\\2511_SER_Test\\01_Rushes\\DRONE\\2025_12_03\\HH_GOW_2025_02_20_DRONE_JOFF_001_SER-sdfdsf\\DJI_20250220172316_0041_D_offspeed_29.97.mp4" '
            '-f lavfi -i aevalsrc=0:d=158.45 -shortest -map_metadata -1 -filter_complex [0:0]sidedata=delete,metadata=delete,setpts=PTS-STARTPTS,setparams=range=tv:color_primaries=bt709:color_trc=bt709:colorspace=bt709,format=yuv422p,scale=w=1920:h=1080:flags=lanczos,fps=25,format=yuv420p,setparams=field_mode=prog:range=tv:color_primaries=bt709:color_trc=bt709:colorspace=bt709,setsar=r=1:max=1[vstr1] '
            '-map [vstr1] -an  -f mxf -g 12.5 -vsync cfr -preset slow -c:v libx264 -b:v 2500k -r 25/1 -color_primaries bt709 -color_trc bt709 -colorspace bt709 -color_range tv -field_order progressive  '
            '-timecode 00:00:00:00   -max_muxing_queue_size 700 -map_metadata -1 -metadata creation_time=now  - | '
            '"\\\\seriouslto\\FFastrans_Working_Version\\FFAStrans-Public-1.4.2\\processors\\mxf_tools\\bmxtranswrap.exe" --log-level 0 --track-map  -l '
            '"\\\\seriouslto\\FFastrans_Working_Version\\Cache\\20251210-0152-0779-54d9-e1e50b926c2a\\20251210-2217-5438-7702-7f945b344665\\142-15-16~251210221912091~23316~20251031-1303-2212-12f8-1a02078a73c5~enc_av_mp4~bmxtrans.txt" '
            '-o "\\\\seriouslto\\FFastrans_Working_Version\\Cache\\20251210-0152-0779-54d9-e1e50b926c2a\\20251210-2217-5438-7702-7f945b344665\\142-15-16~251210221912090~23316~20251031-1303-2212-12f8-1a02078a73c5~enc_av_mp4.mxf" -'
        )
        
        # Create BMX command file with the new content
        bmx_cmd_content = (
            '"\\\\seriouslto\\FFastrans_Working_Version\\FFAStrans-Public-1.4.2\\avid_tools\\0.9\\bmx\\bmxtranswrap.exe" '
            '-t avid --umid-type uuid --project "ffastrans" --clip "DJI_20250220172316_0041_D_offspeed_29.97" --import  '
            '"file://192.168.2.11/2025_01_Dump/Simon_Ffastrans_Dev/2025/02_Jobs/2511_SER_Test/01_Rushes/DRONE/2025_12_03/HH_GOW_2025_02_20_DRONE_JOFF_001_SER-sdfdsf/DJI_20250220172316_0041_D_offspeed_29.97.mp4"   '
            '-o "\\\\seriousnexis\\zFfastrans_Testing_Media\\Avid MediaFiles\\MXF\\FFASTRANS03.5010\\DJI_20250220172316_0041_D_offspeed_29.97_162ca855-985a-4b01-a586-c0e7b8b3ed5f_0_H264GPU" '
            '--tag Camroll "" -'
        )
        
        cmd_file = Path(self.temp_dir.name) / "cmd_bmx.txt"
        cmd_file.write_text(ffmpeg_cmd_with_bmx)
        
        bmx_file = Path(self.temp_dir.name) / "bmx_cmd.txt"
        bmx_file.write_text(bmx_cmd_content)
        
        result = self.run_script(str(cmd_file), bmx_cmd_file=str(bmx_file))
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        # Verify the actual bmx_cmd_content is included in the output
        self.assertIn("-t avid --umid-type uuid --project \"ffastrans\" --clip \"DJI_20250220172316_0041_D_offspeed_29.97\"", result.stdout)
        # Verify metadata creation_time is preserved
        self.assertIn("-metadata creation_time=now", result.stdout)
    
    def test_fx6_sony_bmx_with_hwupload_cuda(self):
        """Test FX6 SONY complex command with bmxtranswrap, hwupload_cuda, and custom options"""
        # Real production command from FFAStrans workflow
        ffmpeg_cmd = (
            '"\\\\seriouslto\\FFastrans_Working_Version\\FFAStrans-Public-1.4.2\\processors\\FFMpeg\\x64\\ffmpeg.exe"  '
            '-analyzeduration 33554432  -stats_period 1.7 -hide_banner  -reinit_filter 0 '
            '-i "\\\\192.168.2.11\\2025_01_Dump\\Simon_Ffastrans_Dev\\2025\\02_Jobs\\2511_SER_Test\\01_Rushes\\SONY\\2025_12_03\\250304_TFL8_ANB_EO_FX6_001\\XDROOT\\Clip\\FX6_EO_2325.MXF" '
            '-shortest -map_metadata -1 '
            '-filter_complex [0:0]sidedata=delete,metadata=delete,setpts=PTS-STARTPTS,setparams=range=tv:color_primaries=bt709:color_trc=1:colorspace=bt709,colorspace=fast=1:ispace=bt709:itrc=1:iprimaries=bt709:all=bt709,fps=25,scale=sws_dither=x_dither,format=yuv420p,setparams=field_mode=prog:range=tv:color_primaries=bt709:color_trc=bt709:colorspace=bt709,setsar=r=1:max=1[vstr1] '
            '-map [vstr1] '
            '-filter_complex "[0:1]asidedata=delete,ametadata=delete,pan=1|c0=c0[a1],[0:2]asidedata=delete,ametadata=delete,pan=1|c0=c0[a2],[0:3]asidedata=delete,ametadata=delete,pan=1|c0=c0[a3],[0:4]asidedata=delete,ametadata=delete,pan=1|c0=c0[a4],[0:5]asidedata=delete,ametadata=delete,pan=1|c0=c0[a5],[0:6]asidedata=delete,ametadata=delete,pan=1|c0=c0[a6],[0:7]asidedata=delete,ametadata=delete,pan=1|c0=c0[a7],[0:8]asidedata=delete,ametadata=delete,pan=1|c0=c0[a8],[a1]amerge=1[astr1],[a2]amerge=1[astr2],[a3]amerge=1[astr3],[a4]amerge=1[astr4],[a5]amerge=1[astr5],[a6]amerge=1[astr6],[a7]amerge=1[astr7],[a8]amerge=1[astr8]" '
            '-map "[astr1]" -c:a:0 pcm_s24le -b:a:0 128k -ar:a:0 48000 -map "[astr2]" -c:a:1 pcm_s24le -b:a:1 128k -ar:a:1 48000 '
            '-map "[astr3]" -c:a:2 pcm_s24le -b:a:2 128k -ar:a:2 48000 -map "[astr4]" -c:a:3 pcm_s24le -b:a:3 128k -ar:a:3 48000 '
            '-map "[astr5]" -c:a:4 pcm_s24le -b:a:4 128k -ar:a:4 48000 -map "[astr6]" -c:a:5 pcm_s24le -b:a:5 128k -ar:a:5 48000 '
            '-map "[astr7]" -c:a:6 pcm_s24le -b:a:6 128k -ar:a:6 48000 -map "[astr8]" -c:a:7 pcm_s24le -b:a:7 128k -ar:a:7 48000  '
            '-f mxf -g 12.5 -vsync cfr -preset slow -c:v libx264 -b:v 2500k -r 25/1 '
            '-color_primaries bt709 -color_trc bt709 -colorspace bt709 -color_range tv -field_order progressive  '
            '-timecode 09:21:56:00   -max_muxing_queue_size 700 -map_metadata -1 -metadata creation_time=now  - | '
            '"\\\\seriouslto\\FFastrans_Working_Version\\FFAStrans-Public-1.4.2\\processors\\mxf_tools\\bmxtranswrap.exe" '
            '--log-level 0 --track-map 0-0;1-1;2-2;3-3;4-4;5-5;6-6;7-7 '
            '-l "\\\\seriouslto\\FFastrans_Working_Version\\Cache\\20251210-0152-0779-54d9-e1e50b926c2a\\20251210-2311-4853-5090-5e70ca75e9ea\\142-15-16~251210231202787~29272~20251031-1303-2212-12f8-1a02078a73c5~enc_av_mp4~bmxtrans.txt" '
            '-o "\\\\seriouslto\\FFastrans_Working_Version\\Cache\\20251210-0152-0779-54d9-e1e50b926c2a\\20251210-2311-4853-5090-5e70ca75e9ea\\142-15-16~251210231202786~29272~20251031-1303-2212-12f8-1a02078a73c5~enc_av_mp4.mxf" -'
        )
        
        # BMX command with Avid project settings
        bmx_cmd = (
            '"\\\\seriouslto\\FFastrans_Working_Version\\FFAStrans-Public-1.4.2\\avid_tools\\0.9\\bmx\\bmxtranswrap.exe" '
            '-t avid --umid-type uuid --project "ffastrans" --clip "FX6_EO_2325" --import  '
            '"file://192.168.2.11/2025_01_Dump/Simon_Ffastrans_Dev/2025/02_Jobs/2511_SER_Test/01_Rushes/SONY/2025_12_03/250304_TFL8_ANB_EO_FX6_001/XDROOT/Clip/FX6_EO_2325.MXF" '
            '--transfer-ch urn:smpte:ul:060e2b34040101060e06040101010605  --color-prim urn:smpte:ul:060e2b34040101060e06040101030105  '
            '--coding-eq urn:smpte:ul:060e2b34040101010401010102020000  '
            '-o "\\\\seriousnexis\\zFfastrans_Testing_Media\\Avid MediaFiles\\MXF\\FFASTRANS03.5010\\FX6_EO_2325_3d6bb5ea-66a7-4429-a382-48e2e84fc707_0_H264GPU" '
            '--tag Camroll "" -'
        )
        
        cmd_file = Path(self.temp_dir.name) / "cmd_fx6.txt"
        cmd_file.write_text(ffmpeg_cmd)
        
        bmx_file = Path(self.temp_dir.name) / "bmx_fx6.txt"
        bmx_file.write_text(bmx_cmd)
        
        # Test with: preset p1, cq 20, g 50, bf 3, hwupload_cuda enabled, bmx_cmd_file
        result = self.run_script(
            str(cmd_file),
            additional_options="-preset p1 -cq 20  -g 50 -bf 3",
            bmx_cmd_file=str(bmx_file),
            insert_hwupload_cuda=True
        )
        
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        
        # Verify hwupload_cuda was inserted in the filter_complex
        self.assertIn("hwupload_cuda", result.stdout)
        # Verify BMX command was properly inserted
        self.assertIn("-t avid --umid-type uuid --project \"ffastrans\" --clip \"FX6_EO_2325\"", result.stdout)
        # Verify Avid-specific options are present in BMX command
        self.assertIn("--transfer-ch urn:smpte:ul:060e2b34040101060e06040101010605", result.stdout)
        self.assertIn("--color-prim urn:smpte:ul:060e2b34040101060e06040101030105", result.stdout)
        # Verify the command structure shows pipe to new bmx executable path
        self.assertIn("FFAStrans-Public-1.4.2\\avid_tools\\0.9\\bmx\\bmxtranswrap.exe", result.stdout)
        # Verify metadata creation_time is preserved
        self.assertIn("-metadata creation_time=now", result.stdout)

if __name__ == '__main__':
    unittest.main()
