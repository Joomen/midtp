import socket
import struct
import json
import os
from datetime import datetime
import logging
from typing import Dict, List, Optional

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

class UDPFileServer:
    def __init__(self, host: str = '0.0.0.0', port: int = 12345, buffer_size: int = 65536):
        self.host = host
        self.port = port
        self.buffer_size = buffer_size
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.received_packets: Dict[int, bytes] = {}
        self.current_metadata = None
        self.biggest_sequence = 0
        self.setup_logging()

    def printMissingBlock(self, missing_sequences: List[int]) -> None:
        for i in range(0, self.biggest_sequence):
            if i in missing_sequences:
                print('▒', end="")
            else:
                print('█', end="")
        print(f"{self.biggest_sequence}")

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('udp_server.log', encoding='utf-8')
            ]
        )
        self.logger = logging.getLogger('UDPFileServer')

    def start(self):
        try:
            self.socket.bind((self.host, self.port))
            self.logger.info(f"UDP 서버 시작됨 - {self.host}:{self.port}")

            while True:
                self.handle_incoming_data()

        except Exception as e:
            self.logger.error(f"서버 에러: {str(e)}")
        finally:
            self.socket.close()
            self.logger.info("서버 종료됨")

    def handle_incoming_data(self):
        try:
            data, addr = self.socket.recvfrom(self.buffer_size)

            # 패킷이 너무 작으면 무시
            if len(data) < 8:
                self.logger.warning(f"패킷 크기가 너무 작음: {len(data)} bytes")
                return

            # 패킷 헤더 파싱 (8바이트)
            sequence_number, biggest_sequence = struct.unpack('!II', data[:8])
            chunk_data = data[8:]

            print(f"chunk {sequence_number} / {biggest_sequence}")

            self.biggest_sequence = biggest_sequence
            self.logger.debug(f"패킷 수신: 시퀀스={sequence_number}, 최대시퀀스={biggest_sequence}")

            # 첫 번째 패킷이면 메타데이터 처리
            if sequence_number == 0:
                try:
                    metadata_size = struct.unpack('!I', chunk_data[:4])[0]
                    if metadata_size <= 0 or metadata_size > len(chunk_data) - 4:
                        raise ValueError(f"잘못된 메타데이터 크기: {metadata_size}")

                    metadata_bytes = chunk_data[4:4 + metadata_size]
                    metadata_json = metadata_bytes.decode('utf-8')
                    self.current_metadata = json.loads(metadata_json)
                    self.logger.info(f"메타데이터 수신: {self.current_metadata}")

                    # 실제 파일 데이터 시작 부분 저장
                    file_data = chunk_data[4 + metadata_size:]
                    if file_data:  # 파일 데이터가 있는 경우만 저장
                        self.received_packets[sequence_number] = file_data
                except Exception as e:
                    self.logger.error(f"메타데이터 처리 실패: {str(e)}")
                    return
            else:
                self.received_packets[sequence_number] = chunk_data

            # 누락된 패킷 확인 및 응답
            missing_sequences = self.check_missing_sequences(biggest_sequence)
            if missing_sequences and sequence_number == self.biggest_sequence:
                self.send_ack_packet(addr, missing_sequences)

            # 모든 패킷이 수신되었다면 파일 저장
            if not missing_sequences and len(self.received_packets) > 0:
                self.save_file()
                self.received_packets.clear()
                self.current_metadata = None
                self.biggest_sequence = 0
                self.send_ack_packet(addr, missing_sequences)

        except Exception as e:
            self.logger.error(f"데이터 처리 중 에러 발생: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())

    def check_missing_sequences(self, biggest_sequence: int) -> List[int]:
        """
        누락된 시퀀스 번호 확인
        """
        missing = []
        for i in range(biggest_sequence + 1):
            if i not in self.received_packets:
                missing.append(i)
        return missing

    def send_ack_packet(self, addr: tuple, missing_sequences: List[int]):
        """
        ACK 패킷 전송
        [시퀀스 개수(4바이트)][시퀀스1][시퀀스2]...
        """
        self.printMissingBlock(missing_sequences)
        try:
            ack_data = struct.pack('!I', len(missing_sequences))
            for seq in missing_sequences:
                ack_data += struct.pack('!I', seq)
            self.socket.sendto(ack_data, addr)

            if missing_sequences:
                self.logger.info(f"누락된 패킷 요청: {missing_sequences}")
            else:
                self.logger.info("모든 패킷 정상 수신")
        except Exception as e:
            self.logger.error(f"ACK 전송 중 에러: {str(e)}")

    def save_file(self):
        """
        수신된 파일 저장
        """
        try:
            if not self.current_metadata:
                self.logger.error("메타데이터 없음")
                return

            # 메타데이터에서 정보 추출
            folder_name = self.current_metadata.get('folderName', 'default')
            file_name = self.current_metadata.get('fileName',
                                                  f'file_{datetime.now().strftime("%Y%m%d_%H%M%S")}')

            # 저장 경로 설정
            save_dir = os.path.join('datasets', folder_name, 'images')
            os.makedirs(save_dir, exist_ok=True)

            # 패킷 순서대로 파일 데이터 조립
            file_data = bytearray()
            for i in range(self.biggest_sequence + 1):
                if i in self.received_packets:
                    file_data.extend(self.received_packets[i])
                else:
                    self.logger.error(f"누락된 패킷 발견: {i}")
                    return

            # 파일 저장
            file_path = os.path.join(save_dir, file_name)
            # 경로가 없을 경우 생성
            ensure_dir(save_dir)

            with open(file_path, 'wb') as f:
                f.write(file_data)

            self.logger.info(f"파일 저장 완료: {file_path}")
            file_size = os.path.getsize(file_path)
            self.logger.info(f"파일 크기: {file_size} bytes")

        except Exception as e:
            self.logger.error(f"파일 저장 중 에러: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())


if __name__ == "__main__":
    server = UDPFileServer(port=9000)
    server.start()
