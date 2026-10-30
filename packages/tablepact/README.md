# TablePact — Notebook용 UC 출력 프로토콜

프로젝트가 입력·변환·품질·업무 범위·재처리를 준비한다. Framework는 스펙 검증,
Job Delta 출력·description 적용과 Lakeflow UC 테이블/flow 선언을 연결한다.
실제 Databricks 실행은 아직 검증하지 않았다.

## 스펙 파일

파일 하나에 테이블 하나를 정의한다. 필수 spec은 Framework가 해석하고 선택적 options는
프로젝트가 해석한다. options를 생략하면 빈 매핑이다. SQL 실행·변수 치환·함수 로딩·
자동 설정 병합은 제공하지 않는다.

```yaml
spec:
  kind: delta
  target: production.sales.daily_sales
  description: 일별 매출
  columns:
    - name: sales_date
      type: DATE
      description: 매출 기준일
    - name: amount
      type: DECIMAL(18, 2)
      description: 매출 금액
  partition_by:
    - sales_date
options:
  transform_sql: |
    SELECT sales_date, amount FROM source.sales
```

columns의 순서가 출력 순서다. 이름·native 타입·설명은 필수이고 중복/대소문자 모호성은
거부한다. 테이블 설명도 필수다. partition_by/cluster_by는 선택적이며 동시 지정 불가다.
컬럼 type은 native Spark parser로 해석한다. 복합 타입도 native 타입 문자열로 기술한다.
별도 중첩 컬럼 설정·자동 cast·스키마 진화·NOT NULL·테이블 재생성은 제공하지 않는다.

## 공개 API

```python
from pathlib import Path
from tablepact.spec import load_spec
from tablepact.delta import overwrite
from tablepact.streaming import define

# str은 YAML 내용, Path는 명시적 파일 경로다.
document = load_spec(Path("specs/daily_sales.yaml"))
spec = document.spec
options = document.options

# 프로젝트가 df와 조건식을 준비한다. options의 SQL도 프로젝트가 해석한다.
result = overwrite(df, spec=spec, replace_where="sales_date = DATE '2026-10-30'")
```

SpecDocument(spec, options), ColumnSpec(name, type, description), TableSpec 및
SnapshotFlowSpec/CDCFlowSpec/AppendFlowSpec은 tablepact.contracts에서 명시적으로 import한다.
Python 생성과 YAML 로딩은 같은 타입 검증을 사용한다. 미지정 키/sequence는 Python 실행
인자로 제공할 수 있으므로 해당 필수 설정은 streaming.define 호출 시 최종 검증한다.
__init__.py에서 함수는 재노출하지 않는다. 기존 평면 YAML·output_schema·from_yaml·
streaming.define_* API는 제거했다. 호환 alias는 없다. 출력 함수에는 document.spec을 전달한다.

## Job 출력과 description

kind=delta은 유효한 flow를 지정할 수 없으며 flow 생략과 null은 동일하게 처리한다. 필수 replace_where 문자열을 native에 그대로
전달한다. Framework에서 조건식을 만들거나 DF를 필터하지 않는다. 빈 조건은 거부하며
명시적 TRUE는 전체 교체다. 입력에 없는 기존 조건 내 행도 제거되며 빈 DF도 삭제를 수행할 수 있다.

이름·순서·타입을 입력/기존 대상 metadata와 비교한다. 중첩 StructField의 설명/metadata는
타입 비교에서 제외하되 중첩 nullability·배열/맵 null 허용·native 타입 속성은 유지한다. Delta mode=overwrite,
mergeSchema=false, overwriteSchema=false와 native 제약 설정을 유지한다. 런타임 설정은
변경하지 않고 재시도·감사 저장·내용 스캔을 추가하지 않는다.

데이터 쓰기가 반환한 다음 UC 테이블 설명과 컬럼 comment metadata를 읽고, 스펙과 다른
항목에만 COMMENT ON TABLE/COLUMN을 실행한다. 설명이 같으면 메타데이터 변경 명령은 0개다.
메타데이터 읽기 실패나 선언 컬럼 누락을 전체 갱신으로 대체하지 않고 설명 단계 오류로 알린다.
COMMENT ON COLUMN은 Databricks Runtime 16.1 이상에서 지원된다. Native 타입·권한·
실행 지원은 실제 환경 검증이 필요하다.
설명 SQL 렌더링을 위해 spark.sql.parser.escapedStringLiterals=false를 요구한다.
설명에 ${가 포함되면 spark.sql.variable.substitute=false도 요구한다. 지원하지 않는 설정은
쓰기 전에 거부하며 Framework가 세션 설정을 변경하지 않는다.

설명 적용 실패는 DescriptionApplyError(target, stage="description", data_written=True)로
알린다. 원본 예외를 __cause__에 보존하며 일부 설명이 적용됐을 수 있다. 데이터와 설명을
하나의 트랜잭션으로 간주하지 않고 자동 롤백·재시도하지 않는다. Native 쓰기 자체의 오류는
그대로 전파한다. WriteResult(target, load="replace_where")는 설명까지 적용된 후 반환한다.

## Streaming Table과 flow

kind=streaming은 flow가 필수다. 지원하는 flow.type은 다음과 같다.

| type | 설정 | source |
|---|---|---|
| auto_cdc_from_snapshot | stored_as_scd_type: 2, keys, 선택 history 목록 | latest native version → (batch DF, native version) 또는 None |
| auto_cdc | stored_as_scd_type: 2, keys, sequence_by, 선택 delete/exclusion/history/null 정책 | 인자 없이 streaming DF 반환 |
| append | 추가 SCD2 설정 없음 | 인자 없이 streaming DF 반환 |

```yaml
spec:
  kind: streaming
  target: production.master.customer_history
  description: 고객 상태 이력
  columns:
    - {name: customer_id, type: BIGINT, description: 고객 식별자}
    - {name: status, type: STRING, description: 고객 상태}
    - {name: __START_AT, type: BIGINT, description: 유효 시작 버전}
    - {name: __END_AT, type: BIGINT, description: 유효 종료 버전}
  cluster_by: [customer_id]
  flow:
    type: auto_cdc_from_snapshot
    stored_as_scd_type: 2
    keys: [customer_id]
    track_history_column_list: [status]
```

```python
define(document.spec, source=project_source)
```

SCD2 시스템 컬럼은 최종 출력 스키마에 명시하고 타입을 맞춘다. 입력 DF가 이 컬럼을
공급해야 한다는 의미는 아니다. Framework는 version/sequence 타입을 추론하지 않는다.
Native와의 타입 호환성은 프로젝트가 충족해야 한다.
키·layout·이력 목록은 출력 컬럼을 참조한다. CDC sequence/delete/exclusion은 입력 전용
컬럼을 사용할 수 있으며 출력 컬럼에 포함하도록 강제하지 않는다.

sequence_by는 native 문자열/Column/struct, apply_as_deletes는 문자열/Column을 지원한다.
표현식이 필요하면 define의 명시적 인자 keys/sequence_by/apply_as_deletes/
except_column_list/track_history_column_list로 제공한다. 동일 필드를 스펙과 호출에 함께
지정하면 오류다. 문자열 컬럼 목록은 스펙과 호출 인자 모두 중복/대소문자 모호성을 거부하며,
Spark Column 표현식은 변환하지 않는다. 임의 **options는 지원하지 않는다. ignore_null_updates는 스펙의 bool이며
기본값 false다. SCD2는 현재 2만 지원한다.

Framework는 target의 comment와 컬럼 comment metadata를 포함한 native schema를 선언한다.
기존 target schema도 metadata로 확인한다. source를 선언 시 호출하지 않고 batch/streaming
형태만 평가 시 검사한다. version·None·DF를 그대로 전달하며 필터·컬럼 제거·내용 스캔은 없다.
첫 None/후속 종료 의미와 SCD2/체크포인트 상태는 native Lakeflow가 담당한다.
기존 target 기반 source/flow 이름을 유지한다. 반환 None은 선언 완료이며 실행 성공이 아니다.
callback에서 별도 description 변경을 실행하지 않는다.

## 매개변수 책임과 검증

매개변수 정의·외부 값 읽기·정규화·기간 타입은 프로젝트 책임이다. Framework에는
parameters 모듈, resolve_parameters, ProcessingWindow, ResolvedParameters, ParameterError가 없다.
프로젝트는 완성된 DF·replace_where·source·native 표현식을 출력 API에 제공한다.
Demo의 tablepact_demo.parameters는 프로젝트 자체 구현 예제이며 Framework 계약이 아니다.

로컬 계약·WHL 검증은 native API 대역을 사용한다. 실제 UC description 갱신·권한·Delta
원자성·SCD2 이력·runtime·checkpoint/full refresh 성공을 대신하지 않는다. Demo의 baseline
검증에는 테이블/컬럼 설명 확인도 포함한다.

[Streaming table](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-streaming-table),
[COMMENT ON](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-comment)

## 소스 배치

Framework의 Python 패키지 루트는 `packages/tablepact`이다. 이 디렉터리 바로 아래에
`__init__.py`, 공개 모듈, `_internal/`, `py.typed`가 위치한다. `src/tablepact` 중첩은 없다.
빌드는 runtime 소스만 WHL의 `tablepact/`로 매핑하며 tests·pyproject.toml·README는
import 패키지에 포함하지 않는다. 편집 가능 설치는 상위 packages 디렉터리를 사용한다.

## 출력 리소스 명칭과 향후 View

공개 namespace는 `tablepact.delta`와 `tablepact.streaming`, spec.kind는 `delta`와
`streaming`이다. kind는 저장 포맷이 아니라 Framework의 UC 출력 리소스 구분이다.
streaming은 Lakeflow Streaming Table 선언이며 batch snapshot source도 포함한다.
기존 job/pipeline 모듈과 delta_table/streaming_table kind의 호환 alias는 없다.
Native create_streaming_table·Bundle jobs/pipelines 등 Databricks 고유 명칭은 유지한다.

향후 일반 UC View는 별도 kind=view·view namespace에서 프로젝트가 준비한 쿼리 정의를
등록하는 방향으로 검토한다. target/description/columns는 공통 출력 정보로 유지한다.
flow는 streaming 전용, partition/clustering은 delta/streaming 전용이다. 현재 view kind와
모듈은 지원하지 않으며 파서가 거부한다. Materialized View는 별도 범위다. 미리 사용할 수
있는 빈 구현·View 필드·플러그인 레지스트리는 추가하지 않는다.
